"""
JCQueue Discrete Event Simulator
=================================
Compares two Apache Storm JCQueue scheduling strategies:

  1. FIFO pure      — poll one tuple at a time, O(1), no reordering
  2. FIFO + batch   — poll N tuples, sort by downstream EWMA jitter
                      (lowest-jitter task served first, mirrors
                      TaskJitterComparator), pay a flat reorder_cost
                      in ticks before draining the batch

Event model
-----------
  - Each producer fires ARRIVE events at Poisson-like intervals
    (Uniform[1, arrive_rate]).
  - The consumer fires CONSUME events; it is single-threaded and
    processes consume_cost ticks per tuple.
  - The event heap drives a proper discrete-event loop — arrival and
    consumption advance on independent clocks, so queue depth and
    back-pressure emerge naturally.

Metrics
-------
  Throughput  — events consumed / total ticks (rolling window + overall)
  Latency     — clock-ticks from tuple birth to end-of-consume (P50/P95/P99/avg)
  EWMA jitter — per child-task inter-delivery deviation from its EWMA prediction;
                jitter_t = |actual_inter - ewma_before_update|,
                global_jitter = EWMA(jitter_t) across all child tasks

Usage
-----
    python jcqueue_sim.py                        # defaults
    python jcqueue_sim.py --events 5000 \\
        --batch-n 8 --reorder-cost 5 --alpha 0.1
    python jcqueue_sim.py --plot                 # requires matplotlib
    python jcqueue_sim.py --sweep reorder-cost   # sensitivity sweep
"""

import argparse
import heapq
import random
import statistics
import sys
from dataclasses import dataclass
from typing import List, Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ARRIVE  = 0
CONSUME = 1


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class SimResult:
    name: str
    throughput: float        # events / total ticks
    avg_latency: float       # mean ticks from birth to consume
    p50_latency: float
    p95_latency: float
    p99_latency: float
    avg_jitter: float        # final global EWMA jitter
    tp_windows: List[float]  # throughput snapshot per window
    jit_windows: List[float] # ewma jitter snapshot per window
    lat_all: List[float]     # raw per-event latencies (for CDF)


# ---------------------------------------------------------------------------
# Core simulator
# ---------------------------------------------------------------------------

def simulate(
        *,
        n_producers: int    = 4,
        queue_size: int     = 64,
        arrive_rate: int    = 3,    # max ticks between successive arrivals per producer
        consume_cost: int   = 12,   # ticks to process one tuple
        use_batch: bool     = False,
        batch_n: int        = 8,    # reorder buffer size  (JCQueue: REORDER_BUFFER_SIZE)
        reorder_cost: int   = 5,    # overhead ticks for the batch sort
        alpha: float        = 0.10, # EWMA smoothing factor
        n_events: int       = 5000, # tuples to consume before stopping
        n_tasks: int        = 6,    # distinct downstream task ids
        task_skew: float    = 0.40, # fraction of extra traffic sent to hot task-0
        window_size: int    = 50,   # events between metric snapshots
        seed: Optional[int] = 42,
        name: str           = "sim",
) -> SimResult:
    """
    Run one simulation and return a SimResult.

    The consumer is intentionally slower than the aggregate producer rate
    (consume_cost > arrive_rate) so the queue builds pressure and latency
    accumulates meaningfully.
    """
    rng = random.Random(seed)

    # Task weight distribution: task-0 is hot
    weights = [1.0] * n_tasks
    weights[0] += task_skew * (n_tasks - 1)
    total_w = sum(weights)
    cum_w = [sum(weights[: i + 1]) / total_w for i in range(n_tasks)]

    def pick_task() -> int:
        r = rng.random()
        for i, cw in enumerate(cum_w):
            if r <= cw:
                return i
        return n_tasks - 1

    # Event heap: (time, event_type, payload)
    heap: list = []
    for pid in range(n_producers):
        t = rng.randint(1, max(1, arrive_rate))
        heapq.heappush(heap, (t, ARRIVE, pid))

    queue: List[dict] = []   # {born: int, task: int}
    consumer_free: int = 0
    consumed: int = 0
    produced: int = 0
    produce_limit = int(n_events * 1.5)

    latencies: List[float] = []

    # Per child-task delivery cadence.
    # task_ewma[t]      : EWMA of inter-delivery interval to child task t.
    #                     Seeded on first delivery; no jitter on first sample.
    # task_last_seen[t] : clock tick of the last tuple delivered to task t.
    # jitter per sample  = |actual_inter - ewma_before_update|  (prediction error)
    # global_jitter      : EWMA of per-sample jitter across all child tasks.
    task_ewma: List[float] = [0.0] * n_tasks
    task_last_seen: List[int] = [-1] * n_tasks
    global_jitter: float = 0.0

    # Windowed snapshot accumulators
    tp_windows: List[float] = []
    jit_windows: List[float] = []
    win_consumed: int = 0
    win_start: int = 0

    # Main event loop
    while consumed < n_events:
        if not heap:
            break

        now, etype, payload = heapq.heappop(heap)

        if etype == ARRIVE:
            pid = payload
            if produced < produce_limit:
                if len(queue) < queue_size:
                    queue.append({"born": now, "task": pick_task()})
                    produced += 1

                # Reschedule producer
                nxt = now + max(1, rng.randint(1, arrive_rate))
                heapq.heappush(heap, (nxt, ARRIVE, pid))

                # Wake consumer if idle
                if consumer_free <= now and queue:
                    heapq.heappush(heap, (now, CONSUME, None))

        elif etype == CONSUME:
            if not queue or consumed >= n_events:
                consumer_free = now
                continue

            start = max(now, consumer_free)

            if use_batch:
                take = min(batch_n, len(queue))
                batch = queue[:take]
                del queue[:take]
                if take > 1:
                    # Mirror TaskJitterComparator: sort by child-task EWMA inter-delivery
                    # interval ascending — the most frequent / stable downstream tasks
                    # are served first, smoothing their receive cadence.
                    batch.sort(key=lambda t: task_ewma[t["task"]])
                    start += reorder_cost
            else:
                batch = [queue.pop(0)]

            t_end = start
            for item in batch:
                t_end += consume_cost
                lat = t_end - item["born"]
                latencies.append(float(lat))

                # Update child-task delivery cadence and compute jitter.
                # inter        : actual gap between consecutive deliveries to this task.
                # ewma_pred    : what the EWMA predicted before seeing this sample.
                # jitter       : absolute prediction error  |actual - predicted|.
                # On first delivery the EWMA is seeded with the observed interval
                # (no jitter recorded — there is no prior prediction to compare).
                tk = item["task"]
                if task_last_seen[tk] >= 0:
                    inter = t_end - task_last_seen[tk]
                    ewma_pred = task_ewma[tk]              # prediction BEFORE update
                    task_ewma[tk] = alpha * inter + (1.0 - alpha) * ewma_pred
                    jitter = abs(inter - ewma_pred)        # deviation from prediction
                    global_jitter = alpha * jitter + (1.0 - alpha) * global_jitter
                else:
                    # First delivery: seed EWMA, no jitter sample
                    task_ewma[tk] = float(consume_cost)
                task_last_seen[tk] = t_end

                consumed += 1
                win_consumed += 1

                if win_consumed >= window_size:
                    elapsed = t_end - win_start or 1
                    tp_windows.append(win_consumed / elapsed)
                    jit_windows.append(global_jitter)
                    win_consumed = 0
                    win_start = t_end

                if consumed >= n_events:
                    break

            consumer_free = t_end

            if queue and consumed < n_events:
                heapq.heappush(heap, (consumer_free, CONSUME, None))

    # Flush trailing window
    if win_consumed > 0:
        elapsed = consumer_free - win_start or 1
        tp_windows.append(win_consumed / elapsed)
        jit_windows.append(global_jitter)

    total_time = consumer_free or 1

    def percentile(data: List[float], p: float) -> float:
        if not data:
            return 0.0
        s = sorted(data)
        return s[max(0, int(len(s) * p / 100) - 1)]

    return SimResult(
        name=name,
        throughput=consumed / total_time,
        avg_latency=statistics.mean(latencies) if latencies else 0.0,
        p50_latency=percentile(latencies, 50),
        p95_latency=percentile(latencies, 95),
        p99_latency=percentile(latencies, 99),
        avg_jitter=global_jitter,
        tp_windows=tp_windows,
        jit_windows=jit_windows,
        lat_all=latencies,
    )


# ---------------------------------------------------------------------------
# Text report
# ---------------------------------------------------------------------------

def _bar(val: float, ref: float, width: int = 28) -> str:
    if ref <= 0:
        return "░" * width
    filled = int(width * min(val / ref, 1.0))
    return "█" * filled + "░" * (width - filled)


def _delta(fifo_val: float, batch_val: float, lower_is_better: bool) -> str:
    if fifo_val == 0:
        return "  n/a"
    diff_pct = (batch_val - fifo_val) / fifo_val * 100
    sign = "+" if diff_pct >= 0 else ""
    better = (diff_pct < 0) if lower_is_better else (diff_pct > 0)
    tag = "✓ better" if better else "✗ worse "
    return f"batch {sign}{diff_pct:5.1f}%  ({tag})"


def print_report(fifo: SimResult, batch: SimResult, cfg: dict) -> None:
    W = 74
    sep = "─" * W

    print(f"\n{'JCQueue Discrete Event Simulation':^{W}}")
    print(sep)
    print(
        f"  producers={cfg['n_producers']}  queue_size={cfg['queue_size']}"
        f"  arrive_rate={cfg['arrive_rate']}  consume_cost={cfg['consume_cost']}"
    )
    print(
        f"  batch_n={cfg['batch_n']}  reorder_cost={cfg['reorder_cost']}"
        f"  alpha={cfg['alpha']}  skew={cfg['task_skew']:.0%}"
        f"  events={cfg['n_events']}"
    )
    print(sep)
    print(f"  {'Metric':<28}  {'FIFO pure':>10}  {'Batch+reorder':>13}  Delta")
    print(sep)

    rows = [
        ("Throughput (evt/tick)",  fifo.throughput,   batch.throughput,   False),
        ("Avg latency (ticks)",    fifo.avg_latency,  batch.avg_latency,  True),
        ("P50 latency (ticks)",    fifo.p50_latency,  batch.p50_latency,  True),
        ("P95 latency (ticks)",    fifo.p95_latency,  batch.p95_latency,  True),
        ("P99 latency (ticks)",    fifo.p99_latency,  batch.p99_latency,  True),
        ("Avg EWMA jitter",        fifo.avg_jitter,   batch.avg_jitter,   True),
    ]
    for label, fv, bv, lib in rows:
        d = _delta(fv, bv, lib)
        print(f"  {label:<28}  {fv:>10.4f}  {bv:>13.4f}  {d}")

    print(sep)
    print("\n  Latency bars (normalised to FIFO P99):\n")
    ref = fifo.p99_latency or 1.0
    for label, fv, bv in [
        ("P50", fifo.p50_latency, batch.p50_latency),
        ("P95", fifo.p95_latency, batch.p95_latency),
        ("P99", fifo.p99_latency, batch.p99_latency),
    ]:
        print(f"  FIFO  {label}  {_bar(fv, ref)}  {fv:,.0f} ticks")
        print(f"  Batch {label}  {_bar(bv, ref)}  {bv:,.0f} ticks")
        print()

    print(sep)
    n = min(20, len(fifo.tp_windows), len(batch.tp_windows))
    print(f"\n  Throughput windows (first {n}):\n")
    print(f"  {'Win':>4}  {'FIFO':>10}  {'Batch':>10}  {'Delta%':>8}")
    for i in range(n):
        fv = fifo.tp_windows[i]
        bv = batch.tp_windows[i]
        d = (bv - fv) / fv * 100 if fv else 0.0
        sign = "+" if d >= 0 else ""
        print(f"  {i+1:>4}  {fv:>10.5f}  {bv:>10.5f}  {sign}{d:>6.1f}%")

    print(f"\n{sep}\n")


# ---------------------------------------------------------------------------
# Sensitivity sweep
# ---------------------------------------------------------------------------

_SWEEP_RANGES = {
    "reorder-cost": [0, 2, 5, 10, 15, 20],
    "batch-n":      [2, 4, 8, 12, 16],
    "alpha":        [0.05, 0.10, 0.20, 0.30, 0.50],
    "task-skew":    [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
    "consume-cost": [4, 8, 12, 16, 24],
}


def run_sweep(param: str, values: list, base_cfg: dict) -> None:
    print(f"\n{'─'*74}")
    print(f"  Sensitivity sweep: {param}")
    print(f"{'─'*74}")
    print(
        f"  {'Value':>8}  {'FIFO tp':>9}  {'Batch tp':>9}"
        f"  {'FIFO lat':>9}  {'Batch lat':>9}"
        f"  {'FIFO jit':>9}  {'Batch jit':>9}"
    )
    print(f"{'─'*74}")
    for v in values:
        cfg = dict(base_cfg)
        cfg[param.replace("-", "_")] = v
        fifo  = simulate(**cfg, use_batch=False, name="fifo")
        batch = simulate(**cfg, use_batch=True,  name="batch")
        print(
            f"  {str(v):>8}  {fifo.throughput:>9.5f}  {batch.throughput:>9.5f}"
            f"  {fifo.avg_latency:>9.1f}  {batch.avg_latency:>9.1f}"
            f"  {fifo.avg_jitter:>9.3f}  {batch.avg_jitter:>9.3f}"
        )
    print()


# ---------------------------------------------------------------------------
# Matplotlib plots
# ---------------------------------------------------------------------------

def plot_results(fifo: SimResult, batch: SimResult, cfg: dict) -> None:
    try:
        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
    except ImportError:
        print("matplotlib not installed:  pip install matplotlib")
        return

    BLUE  = "#378ADD"
    GREEN = "#1D9E75"

    fig = plt.figure(figsize=(14, 10))
    title = (
        f"JCQueue DES  —  N={cfg['batch_n']}, reorder_cost={cfg['reorder_cost']}, "
        f"alpha={cfg['alpha']}, consume_cost={cfg['consume_cost']}, "
        f"skew={cfg['task_skew']:.0%}, events={cfg['n_events']}"
    )
    fig.suptitle(title, fontsize=11, fontweight="bold")
    gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.42, wspace=0.32)

    n = min(len(fifo.tp_windows), len(batch.tp_windows))
    xs = list(range(1, n + 1))

    # 1. Throughput over time
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.plot(xs, fifo.tp_windows[:n],  label="FIFO pure",     color=BLUE,  lw=1.5)
    ax1.plot(xs, batch.tp_windows[:n], label="Batch+reorder", color=GREEN, lw=1.5, ls="--")
    ax1.set_title("Throughput (evt/tick)", fontsize=11)
    ax1.set_xlabel("Window index")
    ax1.set_ylabel("Events / tick")
    ax1.legend(fontsize=9)
    ax1.grid(alpha=0.3)

    # 2. EWMA Jitter over time
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.plot(xs, fifo.jit_windows[:n],  label="FIFO pure",     color=BLUE,  lw=1.5)
    ax2.plot(xs, batch.jit_windows[:n], label="Batch+reorder", color=GREEN, lw=1.5, ls="--")
    ax2.set_title("EWMA Jitter over time", fontsize=11)
    ax2.set_xlabel("Window index")
    ax2.set_ylabel("Jitter (ticks)")
    ax2.legend(fontsize=9)
    ax2.grid(alpha=0.3)

    # 3. Latency percentile bars
    ax3 = fig.add_subplot(gs[1, 0])
    pct_labels = ["P50", "P95", "P99"]
    fifo_vals  = [fifo.p50_latency,  fifo.p95_latency,  fifo.p99_latency]
    batch_vals = [batch.p50_latency, batch.p95_latency, batch.p99_latency]
    xp = range(len(pct_labels))
    w = 0.35
    ax3.bar([i - w/2 for i in xp], fifo_vals,  width=w, label="FIFO pure",     color=BLUE,  alpha=0.85)
    ax3.bar([i + w/2 for i in xp], batch_vals, width=w, label="Batch+reorder", color=GREEN, alpha=0.85)
    ax3.set_xticks(list(xp))
    ax3.set_xticklabels(pct_labels)
    ax3.set_title("Latency percentiles", fontsize=11)
    ax3.set_ylabel("Ticks")
    ax3.legend(fontsize=9)
    ax3.grid(axis="y", alpha=0.3)

    # 4. Latency CDF
    ax4 = fig.add_subplot(gs[1, 1])
    for res, label, c in [
        (fifo,  "FIFO pure",     BLUE),
        (batch, "Batch+reorder", GREEN),
    ]:
        s = sorted(res.lat_all)
        cdf = [i / len(s) for i in range(len(s))]
        ax4.plot(s, cdf, label=label, color=c, lw=1.5)
    for q in (0.50, 0.95, 0.99):
        ax4.axhline(q, color="gray", lw=0.7, ls=":")
    ax4.set_title("Latency CDF", fontsize=11)
    ax4.set_xlabel("Latency (ticks)")
    ax4.set_ylabel("Cumulative probability")
    ax4.legend(fontsize=9)
    ax4.grid(alpha=0.3)

    out = "jcqueue_sim.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Plot saved → {out}")
    plt.show()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="JCQueue DES: FIFO pure vs FIFO+batch reorder",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--producers",    type=int,   default=4,
                   help="Number of concurrent producer threads")
    p.add_argument("--queue-size",   type=int,   default=64,
                   help="Bounded recvQueue capacity")
    p.add_argument("--arrive-rate",  type=int,   default=3,
                   help="Max inter-arrival ticks per producer (Uniform[1,R])")
    p.add_argument("--consume-cost", type=int,   default=12,
                   help="Ticks to process one tuple (consumer speed)")
    p.add_argument("--batch-n",      type=int,   default=8,
                   help="Reorder buffer size N")
    p.add_argument("--reorder-cost", type=int,   default=5,
                   help="Extra ticks overhead for sorting a batch")
    p.add_argument("--alpha",        type=float, default=0.10,
                   help="EWMA smoothing factor alpha in (0,1)")
    p.add_argument("--events",       type=int,   default=5000,
                   help="Total tuples to consume")
    p.add_argument("--tasks",        type=int,   default=6,
                   help="Number of distinct downstream task ids")
    p.add_argument("--task-skew",    type=float, default=0.40,
                   help="Extra traffic fraction to hot task-0 (0=uniform)")
    p.add_argument("--window",       type=int,   default=50,
                   help="Events per metric snapshot window")
    p.add_argument("--seed",         type=int,   default=42,
                   help="Random seed (0 = non-deterministic)")
    p.add_argument("--plot",         action="store_true",
                   help="Render matplotlib charts (pip install matplotlib)")
    p.add_argument("--sweep",        type=str,   default=None, metavar="PARAM",
                   help=(
                           "Sensitivity sweep over PARAM. Choices: "
                           + ", ".join(_SWEEP_RANGES.keys())
                   ))
    return p


def main() -> None:
    args = build_parser().parse_args()
    seed = args.seed if args.seed != 0 else None

    base_cfg = dict(
        n_producers  = args.producers,
        queue_size   = args.queue_size,
        arrive_rate  = args.arrive_rate,
        consume_cost = args.consume_cost,
        batch_n      = args.batch_n,
        reorder_cost = args.reorder_cost,
        alpha        = args.alpha,
        n_events     = args.events,
        n_tasks      = args.tasks,
        task_skew    = args.task_skew,
        window_size  = args.window,
        seed         = seed,
    )

    if args.sweep:
        param = args.sweep
        if param not in _SWEEP_RANGES:
            print(f"Unknown sweep param '{param}'. Choose from: {list(_SWEEP_RANGES)}")
            sys.exit(1)
        run_sweep(param, _SWEEP_RANGES[param], base_cfg)
        return

    print(f"\nRunning FIFO pure ...", end=" ", flush=True)
    fifo = simulate(**base_cfg, use_batch=False, name="FIFO pure")
    print("done.")

    print(f"Running Batch+reorder (N={args.batch_n}, cost={args.reorder_cost}) ...",
          end=" ", flush=True)
    batch = simulate(**base_cfg, use_batch=True, name="Batch+reorder")
    print("done.")

    print_report(fifo, batch, base_cfg)

    if args.plot:
        plot_results(fifo, batch, base_cfg)


if __name__ == "__main__":
    main()