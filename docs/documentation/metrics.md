---
layout: documentation
title: Metrics
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Storm Metrics</h1>
    <p>Storm exposes a metrics interface to report summary statistics across the full topology.
It's used internally to track the numbers you see in the Nimbus UI console: counts of executes and acks; average process latency per bolt; worker heap usage; and so forth.</p>

<h3>Metric Types</h3>

<p>Metrics have to implement just one method, <code>getValueAndReset</code> -- do any remaining work to find the summary value, and reset back to an initial state. For example, the MeanReducer divides the running total by its running count to find the mean, then initializes both values back to zero.</p>

<p>Storm gives you these metric types:</p>

<ul>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/metric/api/AssignableMetric.java" target="_blank">AssignableMetric</a> -- set the metric to the explicit value you supply. Useful if it's an external value or in the case that you are already calculating the summary statistic yourself.</li>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/metric/api/CombinedMetric.java" target="_blank">CombinedMetric</a> -- generic interface for metrics that can be updated associatively. </li>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/metric/api/CountMetric.java" target="_blank">CountMetric</a> -- a running total of the supplied values. Call <code>incr()</code> to increment by one, <code>incrBy(n)</code> to add/subtract the given number.

<ul>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/metric/api/MultiCountMetric.java" target="_blank">MultiCountMetric</a> -- a hashmap of count metrics.</li>
</ul></li>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/metric/api/ReducedMetric.java" target="_blank">ReducedMetric</a>

<ul>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/metric/api/MeanReducer.java" target="_blank">MeanReducer</a> -- track a running average of values given to its <code>reduce()</code> method. (It accepts <code>Double</code>, <code>Integer</code> or <code>Long</code> values, and maintains the internal average as a <code>Double</code>.) Despite his reputation, the MeanReducer is actually a pretty nice guy in person.</li>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/metric/api/MultiReducedMetric.java" target="_blank">MultiReducedMetric</a> -- a hashmap of reduced metrics.</li>
</ul></li>
</ul>

<h3>Metric Consumer</h3>

<h3>Build your own metric</h3>

<h3>Builtin Metrics</h3>

<p>The <a href="https://github.com/apache/storm/blob/46c3ba7/storm-core/src/clj/backtype/storm/daemon/builtin_metrics.clj" target="_blank">builtin metrics</a> instrument Storm itself.</p>

<p><a href="https://github.com/apache/storm/blob/46c3ba7/storm-core/src/clj/backtype/storm/daemon/builtin_metrics.clj" target="_blank">builtin_metrics.clj</a> sets up data structures for the built-in metrics, and facade methods that the other framework components can use to update them. The metrics themselves are calculated in the calling code -- see for example <a href="https://github.com/apache/storm/blob/46c3ba7/storm-core/src/clj/backtype/storm/daemon/executor.clj#358" target="_blank"><code>ack-spout-msg</code></a>  in <code>clj/b/s/daemon/daemon/executor.clj</code></p>
            </div>
        </div>
    </div>
</div>
<!--Content End-->