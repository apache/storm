#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Simulate network delay/jitter/loss on the worker hosts (supervisor containers)
# using Linux tc/netem. The Storm image has no `tc`, so we inject the qdisc from
# a throwaway helper container that shares each supervisor's network namespace
# (--net container:...) -- no image rebuild, no compose change required.
#
# netem applies to *egress* on each supervisor's eth0, so it shapes ALL traffic
# leaving that container (inter-worker tuples, but also heartbeats to Nimbus/ZK).
# Keep the delay moderate (<= ~150ms) so heartbeats don't time out. With both
# supervisors delayed by D, worker<->worker round-trip latency is ~2*D.
#
# netem's default queue is only 1000 packets; under a high-throughput perf
# topology that buffer overflows at the added delay and drops tuples, which
# collapses TCP and back-pressures the spout to ~0. So we set a large `limit`
# (default 1,000,000 packets) -- big enough to hold rate * delay without drops.
#
# Usage:
#   ./netsim.sh add  [delay_ms] [jitter_ms] [loss_pct] [limit_pkts]  # default 50 10 0 1000000
#   ./netsim.sh show
#   ./netsim.sh clear
#   ./netsim.sh ping                                      # measure RTT between supervisors
#
# Override the shaped containers with TARGETS="..." ./netsim.sh ...
set -euo pipefail

TARGETS="${TARGETS:-cluster-supervisor1-1 cluster-supervisor2-1}"
HELPER_IMAGE="${HELPER_IMAGE:-nicolaka/netshoot}"
IFACE="${IFACE:-eth0}"

inns() { # run a command inside container $1's network namespace with NET_ADMIN
  local c="$1"; shift
  docker run --rm --net "container:${c}" --cap-add NET_ADMIN "${HELPER_IMAGE}" "$@"
}

cmd="${1:-show}"
case "${cmd}" in
  add)
    delay="${2:-50}"; jitter="${3:-10}"; loss="${4:-0}"; limit="${5:-1000000}"
    for c in ${TARGETS}; do
      echo "==> ${c}: netem delay ${delay}ms ${jitter}ms loss ${loss}% limit ${limit} on ${IFACE}"
      inns "${c}" tc qdisc replace dev "${IFACE}" root netem \
        delay "${delay}ms" "${jitter}ms" distribution normal loss "${loss}%" limit "${limit}"
    done
    ;;
  clear)
    for c in ${TARGETS}; do
      echo "==> ${c}: removing netem"
      inns "${c}" tc qdisc del dev "${IFACE}" root 2>/dev/null || true
    done
    ;;
  show)
    for c in ${TARGETS}; do
      echo "==> ${c}:"
      inns "${c}" tc qdisc show dev "${IFACE}"
    done
    ;;
  ping)
    set -- ${TARGETS}
    src="$1"; dst_host="${2:-supervisor2}"
    echo "==> RTT from ${src} to ${dst_host} (5 pings)"
    inns "${src}" ping -c 5 "${dst_host}"
    ;;
  *)
    echo "usage: $0 {add [delay_ms] [jitter_ms] [loss_pct] | show | clear | ping}" >&2
    exit 2
    ;;
esac
