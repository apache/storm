---
layout: default
title: FAQ
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Frequently Asked Questions</h1>
                <div class="faq">
                    <ul class="nav nav-tabs" role="tablist">
                        <li role="presentation" class="active"><a href="#practice" aria-controls="practice" role="tab" data-toggle="tab">Best Practices</a></li>
                        <li role="presentation"><a href="#topology" aria-controls="topology" role="tab" data-toggle="tab">Topology</a></li>
                        <li role="presentation"><a href="#spout" aria-controls="spout" role="tab" data-toggle="tab">Spouts</a></li>
                        <li role="presentation"><a href="#time" aria-controls="time" role="tab" data-toggle="tab">Time Series</a></li>
                    </ul>
                    <div class="tab-content">
                        <div role="tabpanel" class="tab-pane active" id="practice">
                            <h4>What rules of thumb can you give me for configuring Storm+Trident?</h4>
                            <ul>
                                <li>number of workers a multiple of number of machines; parallelism a multiple of number of workers; number of kafka partitions a multiple of number of spout parallelism</li>
                                <li>Use one worker per topology per machine</li>
                                <li>Start with fewer, larger aggregators, one per machine with workers on it</li>
                                <li>Use the isolation scheduler</li>
                                <li>Use one acker per worker -- 0.9 makes that the default, but earlier versions do not.</li>
                                <li>enable GC logging; you should see very few major GCs if things are in reasonable shape.</li>
                                <li>set the trident batch millis to about 50% of your typical end-to-end latency.</li>
                                <li>Start with a max spout pending that is for sure too small -- one for trident, or the number of executors for storm -- and increase it until you stop seeing changes in the flow. You'll probably end up with something near <code>2*(throughput in recs/sec)*(end-to-end latency)</code> (2x the Little's law capacity).</li>
                            </ul>
                            <h4>What are some of the best ways to get a worker to mysteriously and bafflingly die?</h4>
                            <ul>
                                <li>Do you have write access to the log directory</li>
                                <li>Are you blowing out your heap?</li>
                                <li>Are all the right libraries installed on all of the workers?</li>
                                <li>Is the zookeeper hostname still set to localhost?</li>
                                <li>Did you supply a correct, unique hostname -- one that resolves back to the machine -- to each worker, and put it in the storm conf file?</li>
                                <li>Have you opened firewall/securitygroup permissions <em>bidirectionally</em> among a) all the workers, b) the storm master, c) zookeeper? Also, from the workers to any kafka/kestrel/database/etc that your topology accesses? Use netcat to poke the appropriate ports and be sure. </li>
                            </ul>
                            <h4>Halp! I cannot see:</h4>
                            <ul>
                                <li><strong>my logs</strong> Logs by default go to $STORM_HOME/logs. Check that you have write permissions to that directory. They are configured in the logback/cluster.xml (0.9) and log4j/*.properties in earlier versions.</li>
                                <li><strong>final JVM settings</strong> Add the <code>-XX+PrintFlagsFinal</code> commandline option in the childopts (see the conf file)</li>
                                <li><strong>final Java system properties</strong> Add <code>Properties props = System.getProperties(); props.list(System.out);</code> near where you build your topology.</li>
                            </ul>
                            <h4>How many Workers should I use?</h4>
                            <p>The total number of workers is set by the supervisors -- there's some number of JVM slots each supervisor will superintend. The thing you set on the topology is how many worker slots it will try to claim.</p>
                            <p>There's no great reason to use more than one worker per topology per machine.</p>
                            <p>With one topology running on three 8-core nodes, and parallelism hint 24, each bolt gets 8 executors per machine, i.e. one for each core. There are three big benefits to running three workers (with 8 assigned executors each) compare to running say 24 workers (one assigned executor each).</p>
                            <p>First, data that is repartitioned (shuffles or group-bys) to executors in the same worker will not have to hit the transfer buffer. Instead, tuples are deposited directly from send to receive buffer. That's a big win. By contrast, if the destination executor were on the same machine in a different worker, it would have to go send -&gt; worker transfer -&gt; local socket -&gt; worker recv -&gt; exec recv buffer. It doesn't hit the network card, but it's not as big a win as when executors are in the same worker.</p>
                            <p>Second, you're typically better off with three aggregators having very large backing cache than having twenty-four aggregators having small backing caches. This reduces the effect of skew, and improves LRU efficiency.</p>
                            <p>Lastly, fewer workers reduces control flow chatter.</p>
                        </div>
                        <div role="tabpanel" class="tab-pane" id="topology">
                            <h4>Can a Trident topology have Multiple Streams?</h4>
                            <blockquote>Can a Trident Topology work like a workflow with conditional paths (if-else)? e.g. A Spout (S1) connects to a bolt (B0) which based on certain values in the incoming tuple routes them to either bolt (B1) or bolt (B2) but not both.</blockquote>
                            <p>A Trident "each" operator returns a Stream object, which you can store in a variable. You can then run multiple eaches on the same Stream to split it, e.g.: </p>
                            <pre>Stream s = topology.each(...).groupBy(...).aggregate(...) 
Stream branch1 = s.each(..., FilterA) 
Stream branch2 = s.each(..., FilterB)</pre>
                            <p>You can join streams with join, merge or multiReduce.</p>
                            <p>At time of writing, you can't emit to multiple output streams from Trident -- see <a href="https://issues.apache.org/jira/browse/STORM-68" target="_blank">STORM-68</a></p>
                        </div>
                        <div role="tabpanel" class="tab-pane" id="spout">
                            <h4>What is a coordinator, and why are there several?</h4>
                            <p>A trident-spout is actually run within a storm <em>bolt</em>. The storm-spout of a trident topology is the MasterBatchCoordinator -- it coordinates trident batches and is the same no matter what spouts you use. A batch is born when the MBC dispenses a seed tuple to each of the spout-coordinators. The spout-coordinator bolts know how your particular spouts should cooperate -- so in the kafka case, it's what helps figure out what partition and offset range each spout should pull from.</p>
                            <h4>What can I store into the spout's metadata record?</h4>
                            <p>You should only store static data, and as little of it as possible, into the metadata record (note: maybe you <em>can</em> store more interesting things; you shouldn't, though)</p>
                            <h4>How often is the 'emitPartitionBatchNew' function called?</h4>
                            <p>Since the MBC is the actual spout, all the tuples in a batch are just members of its tupletree. That means storm's "max spout pending" config effectively defines the number of concurrent batches trident runs. The MBC emits a new batch if it has fewer than max-spending tuples pending and if at least one <a href="https://github.com/apache/storm/blob/master/conf/defaults.yaml#L115" target="_blank">trident batch interval</a>'s worth of seconds has passed since the last batch.</p>
                            <h4>If nothing was emitted does Trident slow down the calls?</h4>
                            <p>Yes, there's a pluggable "spout wait strategy"; the default is to sleep for a <a href="https://github.com/apache/storm/blob/master/conf/defaults.yaml#L110" target="_blank">configurable amount of time</a></p>
                            <h4>OK, then what is the trident batch interval for?</h4>
                            <p>You know how computers of the 486 era had a <a href="http://en.wikipedia.org/wiki/Turbo_button" target="_blank">turbo button</a> on them? It's like that.</p>
                            <p>Actually, it has two practical uses. One is to throttle spouts that poll a remote source without throttling processing. For example, we have a spout that looks in a given S3 bucket for a new batch-uploaded file to read, linebreak and emit. We don't want it hitting S3 more than every few seconds: files don't show up more than once every few minutes, and a batch takes a few seconds to process.</p>
                            <p>The other is to limit overpressure on the internal queues during startup or under a heavy burst load -- if the spouts spring to life and suddenly jam ten batches' worth of records into the system, you could have a mass of less-urgent tuples from batch 7 clog up the transfer buffer and prevent the $commit tuple from batch 3 to get through (or even just the regular old tuples from batch 3). What we do is set the trident batch interval to about half the typical end-to-end processing latency -- if it takes 600ms to process a batch, it's OK to only kick off a batch every 300ms.</p>
                            <p>Note that this is a cap, not an additional delay -- with a period of 300ms, if your batch takes 258ms Trident will only delay an additional 42ms.</p>
                            <h4>How do you set the batch size?</h4>
                            <p>Trident doesn't place its own limits on the batch count. In the case of the Kafka spout, the max fetch bytes size divided by the average record size defines an effective records per subbatch partition.</p>
                            <h4>How do I resize a batch?</h4>
                            <p>The trident batch is a somewhat overloaded facility. Together with the number of partitions, the batch size is constrained by or serves to define</p>
                            <ol>
                                <li>the unit of transactional safety (tuples at risk vs time)</li>
                                <li>per partition, an effective windowing mechanism for windowed stream analytics</li>
                                <li>per partition, the number of simultaneous queries that will be made by a partitionQuery, partitionPersist, etc;</li>
                                <li>per partition, the number of records convenient for the spout to dispatch at the same time;</li>
                            </ol>
                            <p>You can't change the overall batch size once generated, but you can change the number of partitions -- do a shuffle and then change the parallelism hint</p>
                        </div>
                        <div role="tabpanel" class="tab-pane" id="time">
                            <h4>How do I aggregate events by time?</h4>
                            <p>If have records with an immutable timestamp, and you would like to count, average or otherwise aggregate them into discrete time buckets, Trident is an excellent and scalable solution.</p>
                            <p>Write an <code>Each</code> function that turns the timestamp into a time bucket: if the bucket size was "by hour", then the timestamp <code>2013-08-08 12:34:56</code> would be mapped to the <code>2013-08-08 12:00:00</code> time bucket, and so would everything else in the twelve o'clock hour. Then group on that timebucket and use a grouped persistentAggregate. The persistentAggregate uses a local cacheMap backed by a data store. Groups with many records require very few reads from the data store, and use efficient bulk reads and writes; as long as your data feed is relatively prompt Trident will make very efficient use of memory and network. Even if a server drops off line for a day, then delivers that full day's worth of data in a rush, the old results will be calmly retrieved and updated -- and without interfering with calculating the current results.</p>
                            <h4>How can I know that all records for a time bucket have been received?</h4>
                            <p>You cannot know that all events are collected -- this is an epistemological challenge, not a distributed systems challenge. You can:</p>
                            <ul>
                                <li>Set a time limit using domain knowledge</li>
                                <li>Introduce a <em>punctuation</em>: a record known to come after all records in the given time bucket. Trident uses this scheme to know when a batch is complete. If you for instance receive records from a set of sensors, each in order for that sensor, then once all sensors have sent you a 3:02:xx or later timestamp lets you know you can commit. </li>
                                <li>When possible, make your process incremental: each value that comes in makes the answer more an more true. A Trident ReducerAggregator is an operator that takes a prior result and a set of new records and returns a new result. This lets the result be cached and serialized to a datastore; if a server drops off line for a day and then comes back with a full day's worth of data in a rush, the old results will be calmly retrieved and updated.</li>
                                <li>Lambda architecture: Record all events into an archival store (S3, HBase, HDFS) on receipt. in the fast layer, once the time window is clear, process the bucket to get an actionable answer, and ignore everything older than the time window. Periodically run a global aggregation to calculate a "correct" answer.</li>
                            </ul>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
