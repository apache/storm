---
title: Windowing Support in Core Storm
layout: documentation
documentation: true
---

Storm core has support for processing a group of tuples that falls within a window. Windows are specified with the 
following two parameters,

1. Window length - the length or duration of the window
2. Sliding interval - the interval at which the windowing slides

## Sliding Window

Tuples are grouped in windows and window slides every sliding interval. A tuple can belong to more than one window.

For example a time duration based sliding window with length 10 secs and sliding interval of 5 seconds.

```
........| e1 e2 | e3 e4 e5 e6 | e7 e8 e9 |...
-5      0       5            10          15   -> time
|<------- w1 -->|
        |<---------- w2 ----->|
                |<-------------- w3 ---->|
```

The window is evaluated every 5 seconds and some of the tuples in the first window overlaps with the second one.

Note: The window first slides at t = 5 secs and would contain events received up to the first five secs.

## Tumbling Window

Tuples are grouped in a single window based on time or count. Any tuple belongs to only one of the windows.

For example a time duration based tumbling window with length 5 secs.

```
| e1 e2 | e3 e4 e5 e6 | e7 e8 e9 |...
0       5             10         15    -> time
   w1         w2            w3
```

The window is evaluated every five seconds and none of the windows overlap.

Storm supports specifying the window length and sliding intervals as a count of the number of tuples or as a time duration.

The bolt interface `IWindowedBolt` is implemented by bolts that needs windowing support.

```java
public interface IWindowedBolt extends IComponent {
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
    /**
     * Process tuples falling within the window and optionally emit 
     * new tuples based on the tuples in the input window.
     */
    void execute(TupleWindow inputWindow);
    void cleanup();
}
```

Every time the window activates, the `execute` method is invoked. The TupleWindow parameter gives access to the current tuples
in the window, the tuples that expired and the new tuples that are added since last window was computed which will be useful 
for efficient windowing computations.

Bolts that needs windowing support typically would extend `BaseWindowedBolt` which has the apis for specifying the
window length and sliding intervals.

E.g. 

```java
public class SlidingWindowBolt extends BaseWindowedBolt {
	private OutputCollector collector;
	
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	this.collector = collector;
    }
	
    @Override
    public void execute(TupleWindow inputWindow) {
	  for(Tuple tuple: inputWindow.get()) {
	    // do the windowing computation
		...
	  }
	  // emit the results
	  collector.emit(new Values(computedValue));
    }
}

public static void main(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();
     builder.setSpout("spout", new RandomSentenceSpout(), 1);
     builder.setBolt("slidingwindowbolt", 
                     new SlidingWindowBolt().withWindow(new Count(30), new Count(10)),
                     1).shuffleGrouping("spout");
    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(1);

    StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
	
}
```

The following window configurations are supported.

```java
withWindow(Count windowLength, Count slidingInterval)
Tuple count based sliding window that slides after `slidingInterval` number of tuples.

withWindow(Count windowLength)
Tuple count based window that slides with every incoming tuple.

withWindow(Count windowLength, Duration slidingInterval)
Tuple count based sliding window that slides after `slidingInterval` time duration.

withWindow(Duration windowLength, Duration slidingInterval)
Time duration based sliding window that slides after `slidingInterval` time duration.

withWindow(Duration windowLength)
Time duration based window that slides with every incoming tuple.

withWindow(Duration windowLength, Count slidingInterval)
Time duration based sliding window configuration that slides after `slidingInterval` number of tuples.

withTumblingWindow(BaseWindowedBolt.Count count)
Count based tumbling window that tumbles after the specified count of tuples.

withTumblingWindow(BaseWindowedBolt.Duration duration)
Time duration based tumbling window that tumbles after the specified time duration.

```

## Tuple timestamp and out of order tuples
By default the timestamp tracked in the window is the time when the tuple is processed by the bolt. The window calculations
are performed based on the processing timestamp. Storm has support for tracking windows based on the source generated timestamp.

```java
/**
* Specify a field in the tuple that represents the timestamp as a long value. If this
* field is not present in the incoming tuple, an {@link IllegalArgumentException} will be thrown.
*
* @param fieldName the name of the field that contains the timestamp
*/
public BaseWindowedBolt withTimestampField(String fieldName)
```

The value for the above `fieldName` will be looked up from the incoming tuple and considered for windowing calculations. 
If the field is not present in the tuple an exception will be thrown. Alternatively a [TimestampExtractor](../storm-client/src/jvm/org/apache/storm/windowing/TimestampExtractor.java) can be used to
derive a timestamp value from a tuple (e.g. extract timestamp from a nested field within the tuple).

```java
/**
* Specify the timestamp extractor implementation.
*
* @param timestampExtractor the {@link TimestampExtractor} implementation
*/
public BaseWindowedBolt withTimestampExtractor(TimestampExtractor timestampExtractor)
```


Along with the timestamp field name/extractor, a time lag parameter can also be specified which indicates the max time limit for tuples with out of order timestamps.

```java
/**
* Specify the maximum time lag of the tuple timestamp in milliseconds. It means that the tuple timestamps
* cannot be out of order by more than this amount.
*
* @param duration the max lag duration
*/
public BaseWindowedBolt withLag(Duration duration)
```

E.g. If the lag is 5 secs and a tuple `t1` arrived with timestamp `06:00:05` no tuples may arrive with tuple timestamp earlier than `06:00:00`. If a tuple
arrives with timestamp 05:59:59 after `t1` and the window has moved past `t1`, it will be treated as a late tuple. Late tuples are not processed by default,
just logged in the worker log files at INFO level.

```java
/**
 * Specify a stream id on which late tuples are going to be emitted. They are going to be accessible via the
 * {@link org.apache.storm.topology.WindowedBoltExecutor#LATE_TUPLE_FIELD} field.
 * It must be defined on a per-component basis, and in conjunction with the
 * {@link BaseWindowedBolt#withTimestampField}, otherwise {@link IllegalArgumentException} will be thrown.
 *
 * @param streamId the name of the stream used to emit late tuples on
 */
public BaseWindowedBolt withLateTupleStream(String streamId)

```
This behaviour can be changed by specifying the above `streamId`. In this case late tuples are going to be emitted on the specified stream and accessible
via the field `WindowedBoltExecutor.LATE_TUPLE_FIELD`.


### Watermarks
For processing tuples with timestamp field, storm internally computes watermarks based on the incoming tuple timestamp. Watermark is 
the minimum of the latest tuple timestamps (minus the lag) across all the input streams. At a higher level this is similar to the watermark concept
used by Flink and Google's MillWheel for tracking event based timestamps.

Periodically (default every sec), the watermark timestamps are emitted and this is considered as the clock tick for the window calculation if 
tuple based timestamps are in use. The interval at which watermarks are emitted can be changed with the below api.
 
```java
/**
* Specify the watermark event generation interval. For tuple based timestamps, watermark events
* are used to track the progress of time
*
* @param interval the interval at which watermark events are generated
*/
public BaseWindowedBolt withWatermarkInterval(Duration interval)
```


When a watermark is received, all windows up to that timestamp will be evaluated.

For example, consider tuple timestamp based processing with following window parameters,

`Window length = 20s, sliding interval = 10s, watermark emit frequency = 1s, max lag = 5s`

```
|-----|-----|-----|-----|-----|-----|-----|
0     10    20    30    40    50    60    70
````

Current ts = `09:00:00`

Tuples `e1(6:00:03), e2(6:00:05), e3(6:00:07), e4(6:00:18), e5(6:00:26), e6(6:00:36)` are received between `9:00:00` and `9:00:01`

At time t = `09:00:01`, watermark w1 = `6:00:31` is emitted since no tuples earlier than `6:00:31` can arrive.

Three windows will be evaluated. The first window end ts (06:00:10) is computed by taking the earliest event timestamp (06:00:03) 
and computing the ceiling based on the sliding interval (10s).

1. `5:59:50 - 06:00:10` with tuples e1, e2, e3
2. `6:00:00 - 06:00:20` with tuples e1, e2, e3, e4
3. `6:00:10 - 06:00:30` with tuples e4, e5

e6 is not evaluated since watermark timestamp `6:00:31` is older than the tuple ts `6:00:36`.

Tuples `e7(8:00:25), e8(8:00:26), e9(8:00:27), e10(8:00:39)` are received between `9:00:01` and `9:00:02`

At time t = `09:00:02` another watermark w2 = `08:00:34` is emitted since no tuples earlier than `8:00:34` can arrive now.

Three windows will be evaluated,

1. `6:00:20 - 06:00:40` with tuples e5, e6 (from earlier batch)
2. `6:00:30 - 06:00:50` with tuple e6 (from earlier batch)
3. `8:00:10 - 08:00:30` with tuples e7, e8, e9

e10 is not evaluated since the tuple ts `8:00:39` is beyond the watermark time `8:00:34`.

The window calculation considers the time gaps and computes the windows based on the tuple timestamp.

## Guarantees
The windowing functionality in storm core currently provides at-least once guarentee. The values emitted from the bolts
`execute(TupleWindow inputWindow)` method are automatically anchored to all the tuples in the inputWindow. The downstream
bolts are expected to ack the received tuple (i.e the tuple emitted from the windowed bolt) to complete the tuple tree. 
If not the tuples will be replayed and the windowing computation will be re-evaluated. 

The tuples in the window are automatically acked when the expire, i.e. when they fall out of the window after 
`windowLength + slidingInterval`. Note that the configuration `topology.message.timeout.secs` should be sufficiently more 
than `windowLength + slidingInterval` for time based windows; otherwise the tuples will timeout and get replayed and can result
in duplicate evaluations. For count based windows, the configuration should be adjusted such that `windowLength + slidingInterval`
tuples can be received within the timeout period.

## Example topology
An example toplogy `SlidingWindowTopology` shows how to use the apis to compute a sliding window sum and a tumbling window 
average.

## Stateful windowing
The default windowing implementation in storm stores the tuples in memory until they are processed and expired from the 
window. This limits the use cases to windows that
fit entirely in memory. Also the source tuples cannot be ack-ed until the window expiry requiring large message timeouts
(topology.message.timeout.secs should be larger than the window length + sliding interval). This also puts extra loads 
due to the complex acking and anchoring requirements.
 
To address the above limitations and to support larger window sizes, storm provides stateful windowing support via `IStatefulWindowedBolt`. 
User bolts should typically extend `BaseStatefulWindowedBolt` for the windowing operations with the framework automatically 
managing the state of the window in the background.

If the sources provide a monotonically increasing identifier as a part of the message, the framework can use this
to periodically checkpoint the last expired and evaluated message ids, to avoid duplicate window evaluations in case of 
failures or restarts. During recovery, the tuples with message ids lower than last expired id are discarded and tuples with 
message id between the last expired and last evaluated message ids are fed into the system without activating any previously
activated windows.
The tuples beyond the last evaluated message ids are processed as usual. This can be enabled by setting
the `messageIdField` as shown below,

```java
topologyBuilder.setBolt("mybolt",
                   new MyStatefulWindowedBolt()
                   .withWindow(...) // windowing configuarations
                   .withMessageIdField("msgid"), // a monotonically increasing 'long' field in the tuple
                   parallelism)
               .shuffleGrouping("spout");
```

However, this option is feasible only if the sources can provide a monotonically increasing identifier in the tuple and the same is maintained
while re-emitting the messages in case of failures. With this option the tuples are still buffered in memory until processed
and expired from the window.

For more details take a look at the sample topology in storm-starter [StatefulWindowingTopology](../examples/storm-starter/src/jvm/org/apache/storm/starter/StatefulWindowingTopology.java) which will help you get started.

### Window checkpointing

With window checkpointing, the monotonically increasing id is no longer required since the framework transparently saves the state of the window periodically into the configured state backend.
The state that is saved includes the tuples in the window, any system state that is required to recover the state of processing
and also the user state.

```java
topologyBuilder.setBolt("mybolt",
                   new MyStatefulPersistentWindowedBolt()
                   .withWindow(...) // windowing configuarations
                   .withPersistence() // persist the window state
                   .withMaxEventsInMemory(25000), // max number of events to be cached in memory
                    parallelism)
               .shuffleGrouping("spout");

```

The `withPersistence` instructs the framework to transparently save the tuples in window along with
any associated system and user state to the state backend. The `withMaxEventsInMemory` is an optional 
configuration that specifies the maximum number of tuples that may be kept in memory. The tuples are transparently loaded from 
the state backend as required and the ones that are most likely to be used again are retained in memory.

The state backend can be configured by setting the topology state provider config,

```java
// use redis for state persistence
conf.put(Config.TOPOLOGY_STATE_PROVIDER, "org.apache.storm.redis.state.RedisKeyValueStateProvider");

```
Currently storm supports Redis and HBase as state backends and uses the underlying state-checkpointing
framework for saving the window state. For more details on state checkpointing see [State-checkpointing](State-checkpointing.html).

Here is an example of a persistent windowed bolt that uses the window checkpointing to save its state. The `initState`
is invoked with the last saved state (user state) at initialization time. The execute method is invoked based on the configured
windowing parameters and the tuples in the active window can be accessed via an `iterator` as shown below.

```java
public class MyStatefulPersistentWindowedBolt extends BaseStatefulWindowedBolt<K, V> {
  private KeyValueState<K, V> state;
  
  @Override
  public void initState(KeyValueState<K, V> state) {
    this.state = state;
   // ...
   // restore the state from the last saved state.
   // ...
  }
  
  @Override
  public void execute(TupleWindow window) {      
    // iterate over tuples in the current window
    Iterator<Tuple> it = window.getIter();
    while (it.hasNext()) {
        // compute some result based on the tuples in window
    }
    
    // possibly update any state to be maintained across windows
    state.put(STATE_KEY, updatedValue);
    
    // emit the results downstream
    collector.emit(new Values(result));
  }
}
```

**Note:** In case of persistent windowed bolts, use `TupleWindow.getIter` to retrieve an iterator over the
events in the window. If the number of tuples in windows is huge, invoking `TupleWindow.get` would
try to load all the tuples into memory and may throw an OOM exception.

**Note:** In case of persistent windowed bolts the `TupleWindow.getNew` and `TupleWindow.getExpired` are currently not supported
and will throw an `UnsupportedOperationException`.

For more details take a look at the sample topology in storm-starter [PersistentWindowingTopology](../examples/storm-starter/src/jvm/org/apache/storm/starter/PersistentWindowingTopology.java)
which will help you get started.
