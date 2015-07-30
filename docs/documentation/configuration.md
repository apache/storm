---
layout: documentation
title: Configuration
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Configuration</h1>
                <p>Storm has a variety of configurations for tweaking the behavior of nimbus, supervisors, and running topologies. Some configurations are system configurations and cannot be modified on a topology by topology basis, whereas other configurations can be modified per topology. </p>
                <p>Every configuration has a default value defined in <a href="https://github.com/apache/storm/blob/master/conf/defaults.yaml">defaults.yaml</a> in the Storm codebase. You can override these configurations by defining a storm.yaml in the classpath of Nimbus and the supervisors. Finally, you can define a topology-specific configuration that you submit along with your topology when using <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/StormSubmitter.html">StormSubmitter</a>. However, the topology-specific configuration can only override configs prefixed with "TOPOLOGY".</p>
                <p>Storm 0.7.0 and onwards lets you override configuration on a per-bolt/per-spout basis. The only configurations that can be overriden this way are:</p>
                <ol>
                    <li>"topology.debug"</li>
                    <li>"topology.max.spout.pending"</li>
                    <li>"topology.max.task.parallelism"</li>
                    <li>"topology.kryo.register": This works a little bit differently than the other ones, since the serializations will be available to all components in the topology. More details on <a href="Serialization.html">Serialization</a>. </li>
                </ol>
                <p>The Java API lets you specify component specific configurations in two ways:</p>
                <ol>
                    <li><em>Internally:</em> Override <code>getComponentConfiguration</code> in any spout or bolt and return the component-specific configuration map.</li>
                    <li><em>Externally:</em> <code>setSpout</code> and <code>setBolt</code> in <code>TopologyBuilder</code> return an object with methods <code>addConfiguration</code> and <code>addConfigurations</code> that can be used to override the configurations for the component.</li>
                </ol>
                <p>The preference order for configuration values is defaults.yaml &lt; storm.yaml &lt; topology specific configuration &lt; internal component specific configuration &lt; external component specific configuration. </p>
                <p><strong>Resources:</strong></p>
                <ul>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/Config.html">Config</a>: a listing of all configurations as well as a helper class for creating topology specific configurations</li>
                    <li><a href="https://github.com/apache/storm/blob/master/conf/defaults.yaml">defaults.yaml</a>: the default values for all configurations</li>
                    <li><a href="Setting-up-a-Storm-cluster.html">Setting up a Storm cluster</a>: explains how to create and configure a Storm cluster</li>
                    <li><a href="Running-topologies-on-a-production-cluster.html">Running topologies on a production cluster</a>: lists useful configurations when running topologies on a cluster</li>
                    <li><a href="Local-mode.html">Local mode</a>: lists useful configurations when using local mode</li>
                </ul>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
