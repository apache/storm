---
layout: default
title: Documentation
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Documentation</h1>
                <div class="faq">
                    <ul class="nav nav-tabs" role="tablist">
                        <li role="presentation" class="active"><a href="#basic" aria-controls="basic" role="tab" data-toggle="tab">Basics of Storm</a></li>
                        <li role="presentation"><a href="#trident" aria-controls="trident" role="tab" data-toggle="tab">Trident</a></li>
                        <li role="presentation"><a href="#setup" aria-controls="setup" role="tab" data-toggle="tab">Setup and deploying</a></li>
                        <li role="presentation"><a href="#intermediate" aria-controls="intermediate" role="tab" data-toggle="tab">Intermediate</a></li>
                        <li role="presentation"><a href="#advance" aria-controls="advance" role="tab" data-toggle="tab">Advanced</a></li>
                    </ul>
                    <div class="tab-content">
                        <div role="tabpanel" class="tab-pane active" id="basic">
                            <ul>
                                <li><a href="http://nathanmarz.github.com/storm" target="_blank">Javadoc</a></li>
                                <li><a href="/documentation/concepts.html">Concepts</a></li>
                                <li><a href="/documentation/configuration.html">Configuration</a></li>
                                <li><a href="/documentation/guaranteeing-message-processing.html">Guaranteeing message processing</a></li>
                                <li><a href="/documentation/fault-tolerance.html">Fault-tolerance</a></li>
                                <li><a href="/documentation/command-line-client.html">Command line client</a></li>
                                <li><a href="/documentation/understanding-parallelism-of-storm-topology.html">Understanding the parallelism of a Storm topology</a></li>
                                <li><a href="/documentation/faq.html">FAQ</a></li>
                            </ul>
                        </div>
                        <div role="tabpanel" class="tab-pane" id="trident">
                            <p>Trident is an alternative interface to Storm. It provides exactly-once processing, "transactional" datastore persistence, and a set of common stream analytics operations.</p>
                            <ul>
                                <li><a href="documentation/trident-tutorial.html">Trident Tutorial</a> -- basic concepts and walkthrough</li>
                                <li><a href="documentation/trident-api-overview.html">Trident API Overview</a> -- operations for transforming and orchestrating data</li>
                                <li><a href="documentation/trident-state.html">Trident State</a> -- exactly-once processing and fast, persistent aggregation</li>
                                <li><a href="documentation/trident-spouts.html">Trident spouts</a> -- transactional and non-transactional data intake</li>
                            </ul>
                        </div>
                        <div role="tabpanel" class="tab-pane" id="setup">
                            <ul>
                                <li><a href="documentation/setting-up-a-Storm-cluster.html">Setting up a Storm cluster</a></li>
                                <li><a href="documentation/local-mode.html">Local mode</a></li>
                                <li><a href="documentation/troubleshooting.html">Troubleshooting</a></li>
                                <li><a href="documentation/running-topologies-on-production-cluster.html">Running topologies on a production cluster</a></li>
                                <li><a href="documentation/maven.html">Building Storm</a> with Maven</li>
                            </ul>
                        </div>
                        <div role="tabpanel" class="tab-pane" id="intermediate">
                            <ul>
                                <li><a href="documentation/serialization.html">Serialization</a></li>
                                <li><a href="documentation/common-patterns.html">Common patterns</a></li>
                                <li><a href="documentation/clojure-dsl.html">Clojure DSL</a></li>
                                <li><a href="documentation/using-non-jvm-languages-with-storm.html">Using non-JVM languages with Storm</a></li>
                                <li><a href="documentation/distributed-rpc.html">Distributed RPC</a></li>
                                <li><a href="documentation/transactional-topologies.html">Transactional topologies</a></li>
                                <li><a href="documentation/kestrel-and-storm.html">Kestrel and Storm</a></li>
                                <!--<li><a href="javascript:void(0);">Direct groupings</a></li>-->
                                <li><a href="documentation/hooks.html">Hooks</a></li>
                                <li><a href="documentation/metrics.html">Metrics</a></li>
                                <!--<li><a href="javascript:void(0);">Lifecycle of a trident tuple</a></li>-->
                            </ul>
                        </div>
                        <div role="tabpanel" class="tab-pane" id="advance">
                            <ul>
                                <li><a href="documentation/defining-non-jvm-language-dsl-for-storm.html">Defining a non-JVM language DSL for Storm</a></li>
                                <li><a href="documentation/multilang-protocol.html">Multilang protocol</a> (how to provide support for another language)</li>
                                <li><a href="documentation/internal-implementation.html">Implementation docs</a></li>
                            </ul>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
