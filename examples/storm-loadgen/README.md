# Storm Load Generation Tools

A set of tools to place an artificial load on a storm cluster to compare against a different storm cluster.  This is particularly helpful when making changes to the data path in storm to see what if any impact the changes had.  This is also useful for end users that want to compare different hardware setups to see what the trade-offs are, although actually running your real topologies is going to be more accurate.

## Methodology
The idea behind all of these tools is to measure the trade-offs between latency, throughput, and cost when processing data using Apache Storm.

When processing data you typically will know a few things.  First you will know about how much data you are going to be processing.  This will typically be a range of values that change throughput the day.  You also will have an idea of how quickly you need the data processed by.  Often this is measured in terms of the latency it takes to process data at some percentile or set of percentiles.  This is because in most use cases the value of the data declines over time, and being able to react to the data quickly is more valuable.  You probably also have a budget for how much you are willing to spend to be able to process this data.  There are always trade-offs in how quickly you can process some data and how efficiently you can processes that data both in terms of resource usage (cost) and latency.  These tools are designed to help you explore that space.

A note on how latency is measured.  Storm typically measures latency from when a message is emitted by a spout until the point it is fully acked or failed (in many versions of storm it actually does this in the acker instead of the spout so it is trying to be a measure of how long it takes for the actual processing, removing as much of the acker overhead as possible).  For these tools we do it differently.  We simulate a throughput and measure the start time of the tuple from when it would have been emitted if the topology could keep up with the load.  In the normal case this should not be an issue, but if the topology cannot keep up with the throughput you will see the latency grow very high compared to the latency reported by storm.

## Tools
### CaptureLoad 

`CaptureLoad` will look at the topologies on a running cluster and store the structure of and metrics about each in a format described below that can be used later to reproduce a similar load on the cluster.

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
| --parallel &lt;MULTIPLIER(:TOPO:COMP)?> | How much to scale the topology up or down in parallelism. The new parallelism will round up to the next whole number. If a topology + component is supplied only that component will be scaled. If topo or component is blank or a `'*'` all topologies or components matched the other part will be scaled. Only 1 scaling rule, the most specific, will be applied to a component. Providing a topology name is considered more specific than not providing one. (defaults to 1.0 no scaling) |
| -r,--report-interval &lt;INTERVAL_SECS> | How long in between reported metrics.  Will be rounded up to the next 10 sec boundary. default 30 |
| --reporter &lt;TYPE:FILE?OPTIONS>  | Provide the config for a reporter to run. See below for more information about these |
| -t,--test-time &lt;MINS> | How long to run the tests for in mins (defaults to 5) |
| --throughput &lt;MULTIPLIER(:TOPO:COMP)?> | How much to scale the topology up or down in throughput. If a topology + component is supplied only that component will be scaled. If topo or component is blank or a `'*'` all topologies or components matched will be scaled. Only 1 scaling rule, the most specific, will be applied to a component. Providing a topology name is considered more specific than not providing one.(defaults to 1.0 no scaling)|
| -w,--report-window &lt;INTERVAL_SECS> | How long of a rolling window should be in each report.  Will be rounded up to the next report interval boundary. default 30|
| --imbalance &lt;MS(:COUNT)?:TOPO:COMP> | The number of ms that the first COUNT of TOPO:COMP will wait before processing.  This creates an imbalance that helps test load aware groupings. By default there is no imbalance unless specificed by the captrue file. |

## ThroughputVsLatency
A word count topology with metrics reporting like the `GenLoad` command.

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
| --splitter-imbalance &lt;MS(:COUNT)?> | The number of ms that the first COUNT splitters will wait before processing.  This creates an imbalance that helps test load aware groupings (defaults to 0:1)|

# Reporters
Reporters provide a way to store various statistics about a running topology. There are currently a few supported reporters

 * `legacy` - report values like ThroughputVsLatency has done in the past
 * `tsv` - tab separated values
 * `csv` - comma separated values
 * `fixed` - a human readable fixed width format

A `fixed` reporter to stdout will be added if no other reporters are writing to stdout or stderr.

All of these types can have their data written out to a file.  To do this add a path after the type.  For example `legacy:./legacy_data` or `tsv:my_run.tsv`. By default the file will be over written unless an option is given to append instead. Options are in a URL like format, with a `?` separating the type:path from the options, and all of the options separated by a `&`.  To append to the file you can do something like `csv:./my_run.csv?append` or  `csv:./my_run.csv?append=true`

Not all options are supported by all reporters.

|Reporter Option| Description | Supported Reporters|
|---------------|-------------|--------------------|
|time | Set the time unit that you want latency and CPU reported in.  This can be from nanoseconds up to seconds.  Most names are supported for the types| legacy, csv, tsv, fixed|
|columns | A comma separated list of columns to output (see below for the metrics supported).  A `*` is replaced by all metrics. Defaults to "start_time", "end_time", "rate", "mean", "99%ile", "99.9%ile", "cores", "mem", "failed", "ids", "congested" | csv, tsv, fixed |
|extraColumns | Like columns but ones that should be added to the defaults instead of replacing them. A `*` is replaced by all metrics. | csv, tsv, fixed |
|meta | An arbitrary string that will appear as a "meta" column at the end.  This helps when appending to files to keep different runs separated | csv, tsv, fixed|
|precision | The number of places after the decimal point to print out.  The default for fixed is 3, all others it is unlimited. | csv, tsv, fixed|
|tee | A boolean saying if in addition to writing to a file should the output be written to stdout too. | csv, tsv, fixed|
|columnWidth | The width of each field | fixed|

There are a lot of different metrics supported

|Metrics Name| Description| In |
|------------|------------|----|
|99%ile| 99th percentile completion latency. | all
|99.9%ile| 99.9th percentile completion latency. | all
|median| Median completion latency. | all
|mean| Mean completion latency. | all
|min| Minimum completion latency. | all
|max| Maximum completion latency. | all
|stddev| Standard Deviation of completion latency. | all
|user_cpu| User space CPU time.| all
|sys_cpu| System space CPU time. | all
|gc_cpu| Amount of CPU time spent in GC as reported by the JVM. | all
|cores| The number of CPU cores used. `(user_cpu + sys_cpu) / time_window`| all
|uptime| The amount of time the oldest topology has been up for. | all
|acked| The number of tuples fully acked as reported by Storm's metrics. | all
|acked_rate| The rate of tuples fully acked as reported by Storm's metrics. | all
|completed| The number of tuples fully acked as reported by the latency histogram metrics. | all
|rate| The rate of tuples fully acked as reported by the latency histogram metrics. | all
|mem| The amount of memory used by the topology in MB, as reported by the JVM. | all
|failed| The number of failed tuples as reported by Storm's metrics. | all
|start_time| The starting time of the metrics window from when the first topology was launched. | all
|end_time| The ending time of the metrics window from the the first topology was launched. | all
|time_window| the length in seconds for the time window. | all
|ids| The topology ids that are being tracked | all
|congested| Components that appear to be congested | all
|storm_version| The version of storm as reported by the client | all
|java_version| The version of java as reported by the client | all
|os_arch| The OS architecture as reported by the client | all
|os_name| The name of the OS as reported by the client | all
|os_version| The version of the OS as reported by the client | all
|config_override| And command line overrides to storm config values | all
|hosts| The number of hosts the monitored topologies are running on| all
|executors| The number of running executors in the monitored topologies | all
|workers| The number of workers the monitored topologies are running on | all
|skipped\_max\_spout| The number of ms in total that the spout reported it skipped trying to emit because of `topology.max.spout.pending`. This is the sum for all spouts and can be used to decide if setting the value higher will likely improve throughput. `congested` reports individual spouts that appear to be slowed down by this to a large degree. | all
|ui\_complete\_latency| This is a special metric, as it is the average completion latency as reported on the ui for `:all-time`. Because it is comes from the UI it does not follow the normal windows.  Within a window the maximum value reported is used.  | all
|target_rate| The target rate in sentences per second for the ThroughputVsLatency topology | ThroughputVsLatency
|spout_parallel| The parallelism of the spout for the `ThroughputVsLatency` topology. | ThroughputVsLatency
|split_parallel| The parallelism of the split bolt for the `ThroughputVsLatency` topology. | ThroughputVsLatency
|count_parallel| The parallelism of the count bolt for the `ThroughputVsLatency` topology. | ThroughputVsLatency
|parallel\_adjust| The adjustment to the parallelism in `GenLoad`. | GenLoad
|topo_parallel| A list of topology/component specific adjustment rules to the parallelism in `GenLoad`. | GenLoad
|throughput_adjust| The adjustment to the throughput in `GenLoad`. | GenLoad
|topo_throughput| A list of topology/component specific adjustment rules to the throughput in `GenLoad`. | GenLoad
|local\_or\_shuffle| true if shuffles were replaced with local or shuffle in GenLoad. | GenLoad
|slow\_execs| A list of topology/component specific adjustment rules to the slowExecutorPattern in `GenLoad`. | GenLoad

There are also some generic rules that you can use for some metrics.  Any metric that starts with `"conf:"` will be the config for that.  It does not include config overrides from the `GenLoad` file.

In addition any metric that ends with `"%ile"` will be the latency at that percentile.


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
| cpuLoad | The number of cores this component needs for resource aware scheduling |
| memoryLoad | The amount of memory in MB that this component needs for resource aware scheduling |
| slowExecutorPattern.slownessMs | an Optional number of ms to slow down the exec + process latency for some of this component (defaults to 0) |
| slowExecutorPattern.count | the number of components to slow down (defaults to 1) | 

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
