---
layout: default
title: Project ideas
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
        <div class="row">
            <div class="col-md-12">
                <h1 class="page-title">Project Ideas</h1>
                <ul>
                    <li><strong>DSLs for non-JVM languages:</strong> These DSL's should be all-inclusive and not require any Java for the creation of topologies, spouts, or bolts. Since topologies are <a href="http://thrift.apache.org/">Thrift</a> structs, Nimbus is a Thrift service, and bolts can be written in any language, this is possible.</li>
                    <li><strong>Online machine learning algorithms:</strong> Something like <a href="http://mahout.apache.org/">Mahout</a> but for online algorithms</li>
                    <li><strong>Suite of performance benchmarks:</strong> These benchmarks should test Storm's performance on CPU and IO intensive workloads. There should be benchmarks for different classes of applications, such as stream processing (where throughput is the priority) and distributed RPC (where latency is the priority). </li>
                </ul>
            </div>
        </div>
    </div>
</div>
<!--Content End-->