# Storm Load Generation Tools

A set of tools to place an artificial load on a storm cluster to compare against a different storm cluster.  This is particularly helpful when making changes to the data path in storm to see what if any impact the changes had.  This is also useful for end users that want to compare different hardware setups to see what the trade-offs are, although actually running your real topologies is going to be more accurate.

## Methodology
The idea behind all of these tools is to measure the trade-offs between latency, throughput, and cost when processing data using Apache Storm.

When processing data you typically will know a few things.  First you will know about how much data you are going to be processing.  This will typically be a range of values that change throughput the day.  You also will have an idea of how quickly you need the data processed by.  Often this is measured in terms of the latency it takes to process data at the some percentile or set of percentiles.  This is because of most use cases the value of the data declines over time, and being able to react to the data quickly is more valuable.  You probably also have a budget for how much you are willing to spend to be able to process this data.  There are always trade-offs in how quickly you can process some data and how efficiently you can processes that data both in terms of resource usage (cost) and latency.  These tools are designed to help you explore that space.

## Tools
### CaptureLoad 

`CaptureLoad` will look at the topologies on a running cluster and store the structure of and metrics about each of theses topologies storing them in a format that can be used later to reproduce a similar load on the cluster.

#### Usage
```
storm jar storm-loadgen.jar org.apache.storm.loadgen.CaptureLoad [options] [topologyName]*
```
|Option| Description|
|-----|-----|
|-a,--anonymize | Strip out any possibly identifiable information|
| -h,--help | Print a help message |
| -o,--output-dir <file> | Where to write (defaults to ./loadgen/)|

#### Limitations
This is still a work in progress.  It does not currently capture CPU or memory usage of a topology.  Resource requests (used by RAS when scheduling) within the topology are also not captured yet, nor is the user that actually ran the topology.

### GenLoad

`GenLoad` will take the files produced by `CaptureLoad` and replay them in a simulated way on a cluster.  It also offers lots of ways to capture metrics about those simulated topologies to be able to compare different software versions of different hardware setups.  You can also make adjustments to the topology before submitting it to change the size or throughput of the topology.

### Usage
```
storm jar storm-loadgen.jar org.apache.storm.loadgen.GenLoad [options] [capture_file]*
```

|Option| Description|
|-----|-----|
| --debug | Print debug information about the adjusted topology before submitting it. |
|-h,--help | Print a help message |
| --local-or-shuffle | Replace shuffle grouping with local or shuffle grouping. |
| --parallel &lt;MULTIPLIER> | How much to scale the topology up or down in parallelism. The new parallelism will round up to the next whole number (defaults to 1.0 no scaling) The total throughput of the topology will not be scaled. |
| -r,--report-interval &lt;INTERVAL_SECS> | How long in between reported metrics.  Will be rounded up to the next 10 sec boundary. default 30 |
| --reporter &lt;TYPE:FILE?OPTIONS>  | Provide the config for a reporter to run. See below for more information about these |
| -t,--test-time &lt;MINS> | How long to run the tests for in mins (defaults to 5) |
| --throughput &lt;MULTIPLIER> | How much to scale the topology up or down in throughput. (defaults to 1.0 no scaling)|
| -w,--report-window &lt;INTERVAL_SECS> | How long of a rolling window should be in each report.  Will be rounded up to the next report interval boundary. default 30|

## ThroughputVsLatency
This is a topology similar to `GenLoad` in most ways, except instead of simulating a load it runs a word count algorithm.

### Usage
```
storm jar storm-loadgen.jar org.apache.storm.loadgen.ThroughputVsLatency [options]
```

|Option| Description|
|-----|-----|
|--counters &lt;NUM>| Number of counter bolts to use (defaults to 1)|
| -h,--help | Print a help message |
| --name <TOPO_NAME> | Name of the topology to run (defaults to wc-test) |
| -r,--report-interval &lt;INTERVAL_SECS>| How long in between reported metrics.  Will be rounded up to the next 10 sec boundary. default 30 |
| --rate &lt;SENTENCES/SEC>| How many sentences per second to run. (defaults to 500) |
| --reporter &lt;TYPE:FILE?OPTIONS>  | Provide the config for a reporter to run. See below for more information about these |
|--splitters &lt;NUM> | Number of splitter bolts to use (defaults to 1) |
| --spouts &lt;NUM>| Number of spouts to use (defaults to 1) |
| -t,--test-time &lt;MINS>| How long to run the tests for in mins (defaults to 5) |
| -w,--report-window &lt;INTERVAL_SECS>| How long of a rolling window should be in each report.  Will be rounded up to the next report interval boundary.|

# Reporters
Reporters provide a way to store various statistics about a running topology. There are currently a few supported reporters

 * legacy - report values like ThroughputVsLatency has done in the past
 * TSV - tab separated values
 * CSV - comma separated values

All of these types can have their data written out to a file.  To do this add a path after the type.  For example `legacy:./legacy_data` or `tsv:my_run.tsv`. By default the file will be over written unless an option is given to append instead. Options are in a URL like format, with a `?` separating the type:path from the options, and all of the options separated by a `&`.  To append to the file you can do something like `csv:./my_run.csv?append` or  `csv:./my_run.csv?append=true`

Not all options are supported by all reporters.

|Reporter Option| Description | Supported Reporters|
|---------------|-------------|--------------------|
|time | Set the time unit that you want latency and CPU reported in.  This can be from nanoseconds up to seconds.  Most names are supported for the types| legacy, csv, tsv|
|columns | A comma separated list of columns to output (see below for the metrics supported).  Defaults to "start_time", "end_time", "completion_rate", "mean", "99%ile", "99.9%ile", "cores", "mem", "failed" | csv, tsv |
|extraColumns | Like columns but ones that should be added to the defaults instead of replacing them (this is mostly for convenience) | csv, tsv |
|meta | An arbitrary string that will appear as a "meta" column at the end.  This helps when appending to files to keep different runs separated | csv, tsv|

There are a lot of different metrics supported

|Metrics Name| Description|
|------------|------------|
|99%ile| 99th percentile completion latency. |
|99.9%ile| 99.9th percentile completion latency. |
|median| Median completion latency. |
|mean| Mean completion latency. |
|min| Minimum completion latency. |
|max| Maximum completion latency. |
|stddev| Standard Deviation of completion latency. |
|user_cpu| User space CPU time.|
|sys_cpu| System space CPU time. |
|gc_cpu| Amount of CPU time spent in GC as reported by the JVM. |
|cores| The number of CPU cores used. `(user_cpu + sys_cpu) / time_window`|
|uptime| The amount of time the oldest topology has been up for. |
|acked| The number of tuples fully acked as reported by Storm's metrics. |
|rate| The rate of tuples fully acked as reported by Storm's metrics. |
|completed| The number of tuples fully acked as reported by the latency histogram metrics. |
|completion_rate| The rate of tuples fully acked as reported by the latency histogram metrics. |
|mem| The amount of memory used by the topology in MB, as reported by the JVM. |
|failed| The number of failed tuples as reported by Storm's metrics. |
|start_time| The starting time of the metrics window from when the first topology was launched.
|end_time| The ending time of the metrics window from the the first topology was launched.
|time_window| the length in seconds for the time window. |
|ids| The topology ids that are being tracked |

# Captured Load File Format
The file format used with `CaptureLoad` and `GenLoad` is based off of the flux file format, but with some extensions and omissions.

At a top level the supported options keys are

| Config | Description |
|--------|-------------|
| name | The name of the topology.  If not given the base name of the file will be used. |
| config | A map of String to Object configs to use when submitting the topology. |
| spouts | A list of spouts for the topology. |
| bolts | A list of bolts in the topology. |
| streams | A list of streams connecting different components in the topology. |

## Spouts and Bolts

Spouts and bolts have the same format.

| Config | Description |
|--------|-------------|
| id | The id of the bolt or spout.  This should be unique within the topology |
| parallelism | How many instances of this component should be a part of the topology |
| streams | The streams that are produced by this bolt or spout |

### Output Streams

This is not a part of flux.  It defines the output of a bolt or spout.

| Config | Description |
|--------|-------------|
| streamId | The ID of the stream being output.  The default is "default" |
| rate | This is a map describing the rate at which messages are output on this stream. |

The rate has at least a `mean` value.  If you want the rate to vary a bit over time you can also include a Standard Deviation with `stddev` and a `min` and `max` value.  The actual rates selected will follow a Gaussian distribution within those bounds.

## (Input) Streams

The streams that connect components together has the form.

| Config | Description |
|--------|-------------|
| from | the component id the stream is coming from |
| to | the component id the stream is going to |
| grouping | This is a map that defines the grouping used |
| grouping.type | the type of grouping including `SHUFFLE`, `FIELDS`, `ALL`, `GLOBAL`, `LOCAL_OR_SHUFFLE`, `NONE`, or `PARTIAL_KEY`.  defaults to `SHUFFLE` |
| grouping.streamId | the id of the stream (default is "default") |
| execTime | a distribution of the amount of time in milliseconds that execution of this component takes (execute latency). |
| processTime | a distribution of the amount of time in milliseconds that processing of a tuple takes (process latency). |

The `execTime` and `processTime` values follow the same pattern as the `OutputStream` `rate`.  A `mean` values is required, but `stddev`, `min`, and `max` may also be given.
