---
title: Message Timeout Report
layout: documentation
documentation: true
---
When a tuple tree is not fully processed, Storm calls `fail()` on the spout with the message id. The call does not
say whether a bolt failed a tuple of the tree or the tree [timed out](Guaranteeing-message-processing.html). With the
message timeout report enabled, the spout's worker logs the timeouts of its spouts, with the id of the tree, the
spout task that emitted it and the message id.

## Enabling the report

The report is off by default. To turn it on, set `topology.message.timeout.report` in the topology configuration:

```yaml
topology.message.timeout.report: timeout-events
```

| Value | Effect |
|-------|--------|
| `disabled` | The reporter is off (default). |
| `timeout-events` | Report timeouts declared by this worker's spouts. This mode reports spout timeouts only. Tracking deliveries on bolts would require a separate opt-in. |

Storm declares a timeout only for a tuple that a spout emits with a message id, in a topology with ackers, while
`topology.enable.message.timeouts` is true.

The logger `org.apache.storm.daemon.worker.MessageTimeoutReporter` writes the lines at `INFO`. The configuration key
turns the report on. The level that applies to that logger decides whether the lines reach the log: at `WARN` or
above, neither the events nor the summary line described below reach it. Storm's default worker logging
configuration logs at `INFO`. To change the level of one logger while the topology runs, see
[Dynamic Log Level Settings](dynamic-log-level-settings.html).

## The timeout event

The message of a timeout event has this form:

```
timeout root=00000000075bcd15 task=7 component=url-spout id="https://example.org/a"
```

* `root` is the id of the tuple tree, as 16 hexadecimal digits. Each emit of the spout starts a tree with a new
  random id. A replay of the same message is a new emit, with a new random id.
* `task` is the id of the spout task that emitted the tree, and `component` is the id of that spout. They identify
  only the origin of the tree. The component id is written without quotes, with the same escapes as the message id.
  A space in the component id is written `\u{20}`, so a component `s truncated=true` appears as
  `component=s\u{20}truncated=true`. The component id is cut after 64 UTF-16 code units, counted after the escapes.
  No field marks a cut component id.
* `id` is the message id that the spout passed to `emit`.

If the spout receives an explicit fail for the root before it declares a timeout, the reporter emits no timeout
event for that root.

At `INFO`, the first word of the log message identifies the line type: `timeout`, or `summary` (see
[Missing events](#missing-events)). Readers should ignore unfamiliar line types and fields. In a timeout event,
`id` is always the last field.

## Finding an event

Search the worker log for a part of the message id, written as it appears in the log, or for the whole token
`root=00000000075bcd15`. Every `root` token has the same length, so a whole token does not match part of another
tree's token.

The logviewer's [log search](Logs.html) takes a search string of at most 1024 bytes in UTF-8. An id with non-ASCII
characters can fit in the event and exceed that limit, so search for a part of it.

## The message id

Only a `String` message id is written. For any other type, the event ends with `id=<not supported>`, and the report
calls no method of the object.

A `String` id ends at the closing quote, which is the last character of the message, so double quotes inside the id
are written as they are. Some other characters are written as escapes. The escapes keep an event on one line and
make invisible characters visible:

* a backslash is written `\\`;
* control characters (such as line feed and tab), format characters (such as the bidirectional marks and the zero
  width joiner), the line and paragraph separators and unpaired surrogates are written `\u{hex}`, where `hex` is the
  code point in lowercase hexadecimal digits without leading zeros. A line feed is `\u{a}`.

An id takes at most 1024 UTF-16 code units in the event, counted after the escapes. A longer id is cut, and two
fields before `id` say so:

```
timeout root=00000000075bcd15 task=7 component=url-spout truncated=true originalLength=5000 id="<the first part of the id>"
```

`originalLength` is the length of the whole id in UTF-16 code units, before the escapes. The two fields count
toward the 1024 units. In this example they take 34 units, which leaves at most 990 units for the id. The cut never
splits a surrogate pair or an escape.

## Missing events

The spout hands each event to a queue instead of writing it to the log. A thread of the worker, the reporter, writes
the queued events. A log line carries the time of its write, which is later than the timeout.

All spouts on a worker share a queue with 1024 slots for timeout events. The reporter takes up to 1024 events from
the queue per cycle, then waits one second before starting the next cycle. A burst of timeouts or slow log writes
can fill the queue. At the end of each cycle, the reporter checks its counters and writes a summary if either
counter has increased since the last successful logging call for a summary. For example:

```
summary events_dropped=3
```

* `events_dropped` counts events that found no free slot.
* `write_failed` counts logging calls that threw an exception back to the reporter. With Storm's default worker
  configuration, the rolling file and syslog appenders handle their errors internally. If events appear to be
  missing, check `worker.log.err` for messages from Log4j's Status Logger. Log4j may suppress repeated error
  messages.

If a `RuntimeException` stops the reporter, the worker keeps running. The reporter makes one attempt to log
`The message timeout report is off until the worker restarts` at `ERROR`. It produces no further timeout events or
summaries until the worker restarts.

Each number counts from the last summary whose logging call returned, whether or not the level of the logger let
that summary reach the log. A summary leaves out a counter at zero.

A log without summary lines does not show that every event was written. The level of the logger hides summary lines
as it hides events. Events still queued when the worker stops can be lost.
