---
title: Resources
layout: documentation
documentation: true
---

<div class="row">
	<div class="col-md-12">	
		<div class="resources">
			<ul class="nav nav-tabs" role="tablist">
		        <li role="presentation" class="active"><a href="#talks" aria-controls="talks" role="tab" data-toggle="tab">Talks</a></li>
		        <li role="presentation"><a href="#slideshows" aria-controls="slideshows" role="tab" data-toggle="tab">Slideshows</a></li>
		    </ul>
		    
			<div class="tab-content">
				<div role="tabpanel" class="tab-pane active" id="talks">
					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/LpNbjXFPyZ0" frameborder="0" allowfullscreen></iframe>
						    </div>
						    <div class="col-md-6">
						    	<h3>Storm: distributed and fault-tolerant realtime computation from 
								<span>
									nathanmarz
								</span>
								</h3>
								<div>
									<p>Published on Nov 29, 2013
				This is a technical architect's case study of how Loggly has employed the latest social-media-scale technologies as the backbone ingestion processing for our multi-tenant, geo-distributed, and real-time log management system. This presentation describes design details of how we built a second-generation system fully leveraging AWS services including Amazon Route 53 DNS with heartbeat and latency-based routing, multi-region VPCs, Elastic Load Balancing, Amazon Relational Database Service, and a number of pro-active and re-active approaches to scaling computational and indexing capacity.
				The talk includes lessons learned in our first generation release, validated by thousands of customers; speed bumps and the mistakes we made along the way; various data models and architectures previously considered; and success at scale: speeds, feeds, and an unmeltable log processing engine.</p>
								</div>
						    </div>
						</div>
					</div>

					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						    	<h3>Apache Storm for Real-Time Processing in Hadoop
								<span>
									Hortonworks
								</span>
								</h3>
								<div>
									<p>Published on Jan 29, 2014
			In this video, we cover:

			-- 5 key benefits of Apache Storm
			-- The reasons for adding Apache Storm to Hortonworks Data Platform
			-- How Apache Hadoop YARN opened the door for integration of Storm into Hadoop
			-- Two general use case patterns with Storm and specific uses in transportation and advertising
			-- How Ambari will provide a single view and common operational platform for enterprises to distribute cluster resources across different workloads</p>
								</div>
						    </div>
						    <div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/l1MM_SHDrPY" frameborder="0" allowfullscreen></iframe>
						    </div>
						</div>
					</div>

					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/hVO5nbxnBkU" frameborder="0" allowfullscreen></iframe>
						    </div>
						    <div class="col-md-6">
						        <h3>Real-Time Big Data Analytics with Storm
								<span>
									Aerospike
								</span>
								</h3>
						        <div>
						        	<p>Published on Oct 12, 2013
			This talk provides an overview of the open source Storm system for processing Big Data in realtime. The talk starts with an overview of the technology, including key components: Nimbus, Zookeeper, Topology, Tuple, Trident. The presentation then dives into the complex Big Data architecture in which Storm can be integrated. The result is a compelling stack of technologies including integrated Hadoop clusters, MPP, and NoSQL databases.

			The presentation then reviews real world use cases for realtime Big Data analytics. Social updates, in particular real-time news feeds on sites like Twitter and Facebook, benefit from Storm's capacity to process benefits from distributed logic of streaming. Another case study is financial compliance monitoring, where Storm plays a primary role in reducing the market data to a useable subset in realtime. In a final use case, Storm is crucial to collect rich operational intelligence, because it builds multidimensional stats and executes distributed queries.</p>
						        </div>
							</div>
						</div>
					</div>

					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <h3>ETE 2012 - Nathan Marz on Storm
								<span>
									ChariotSolutions
								</span>
								</h3>
						        <div>
						        	<p>Published on May 15, 2012
			"Storm makes it easy to write and scale complex realtime computations on a cluster of computers, doing for realtime processing what Hadoop did for batch processing. Storm guarantees that every message will be processed. And it's fast -- you can process millions of messages per second with a small cluster. Best of all, you can write Storm topologies using any programming language. Storm was open-sourced by Twitter in September of 2011 and has since been adopted by numerous companies around the world.
			Storm provides a small set of simple, easy to understand primitives. These primitives can be used to solve a stunning number of realtime computation problems, from stream processing to continuous computation to distributed RPC. In this talk you'll learn:

			- The concepts of Storm: streams, spouts, bolts, and topologies
			- Developing and testing topologies using Storm's local mode
			- Deploying topologies on Storm clusters
			- How Storm achieves fault-tolerance and guarantees data processing
			- Computing intense functions on the fly in parallel using Distributed RPC
			- Making realtime computations idempotent using transactional topologies
			- Examples of production usage of Storm</p>
						        </div>
							</div>
							<div class="col-md-6">
						        <iframe width="420" height="315" src="https://www.youtube.com/embed/bdps8tE0gYo" frameborder="0" allowfullscreen></iframe>
						    </div>
						</div>
					</div>

					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/od8U-XijzlQ" frameborder="0" allowfullscreen></iframe>
						    </div>
						    <div class="col-md-6">
						        <h3>Andrew Montalenti & Keith Bourgoin - Real-time streams and logs with Storm and Kafka
								<span>
									PyData
								</span>
								</h3>
						        <div>
						        	<p>Published on Jun 12, 2014
			PyData SV 2014 
			Some of the biggest issues at the center of analyzing large amounts of data are query flexibility, latency, and fault tolerance. Modern technologies that build upon the success of "big data" platforms, such as Apache Hadoop, have made it possible to spread the load of data analysis to commodity machines, but these analyses can still take hours to run and do not respond well to rapidly-changing data sets.

			A new generation of data processing platforms -- which we call "stream architectures" -- have converted data sources into streams of data that can be processed and analyzed in real-time. This has led to the development of various distributed real-time computation frameworks (e.g. Apache Storm) and multi-consumer data integration technologies (e.g. Apache Kafka). Together, they offer a way to do predictable computation on real-time data streams.

			In this talk, we will give an overview of these technologies and how they fit into the Python ecosystem. This will include a discussion of current open source interoperability options with Python, and how to combine real-time computation with batch logic written for Hadoop. We will also discuss Kafka and Storm's alternatives, current industry usage, and some real-world examples of how these technologies are being used in production by Parse.ly today.</p>
						        </div>
							</div>
						</div>
					</div>

					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <h3>Yahoo talks about Spark vs. Storm
								<span>
									Jim Scott
								</span>
								</h3>
						        <div>
						        	<p>Published on Sep 18, 2014
			Bobby Evans and Tom Graves, the engineering leads for Spark and Storm development at Yahoo will talk about how these technologies are used on Yahoo's grids and reasons why to use one or the other.

			Bobby Evans is the low latency data processing architect at Yahoo. He is a PMC member on many Apache projects including Storm, Hadoop, Spark, and Tez. His team is responsible for delivering Storm as a service to all of Yahoo and maintaining Spark on Yarn for Yahoo (Although Tom really does most of that work).

			Tom Graves a Senior Software Engineer on the Platform team at Yahoo. He is an Apache PMC member on Hadoop, Spark, and Tez. His team is responsible for delivering and maintaining Spark on Yarn for Yahoo.</p>
						        </div>
							</div>
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/uJ5rdAPHE1w" frameborder="0" allowfullscreen></iframe>
						    </div>
						    
						</div>
					</div>

					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/5F0eQ7mkpTU" frameborder="0" allowfullscreen></iframe>
						    </div>
						    <div class="col-md-6">
						        <h3>Apache Storm Deployment and Use Cases by Spotify Developers
								<span>
									Hakka Labs
								</span>
								</h3>
						        <div>
						        	<p>Published on Apr 3, 2014
			This talk was presented at the New York City Storm User Group hosted by Spotify on March 25, 2014. More info here: http://www.hakkalabs.co/articles/stor...
			This is the first time that a Spotify engineer has spoken publicly about their deployment and use cases for Storm! In this talk, Software Engineer Neville Li describes:

			Real-time features developed using Storm and Kafka including recommendations, social features, data visualization and ad targeting

			Architecture

			Production integration

			Best practices for deployment</p>
						        </div>
							</div>
						</div>
					</div>

				</div>

				<div role="tabpanel" class="tab-pane" id="slideshows">
					<div class="row" style="padding-left: 45px;">
						<div class="col-md-6 brick">
							<h2>Apache storm vs. Spark Streaming
								<span>
									P. Taylor Goetz
								</span>
							</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/hZmAFT4CZ19WEM" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/ptgoetz/apache-storm-vs-spark-streaming" title="Apache storm vs. Spark Streaming" target="_blank">Apache storm vs. Spark Streaming</a> </strong> from <strong><a href="//www.slideshare.net/ptgoetz" target="_blank">P. Taylor Goetz</a></strong> </div>
						</div>

						<div class="col-md-6 brick">
							<h2>Scaling Apache Storm - Strata + Hadoop World 2014
								<span>
									P. Taylor Goetz
								</span>
							</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/NRMYq1985xMCWv" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/ptgoetz/scaling-apache-storm-strata-hadoopworld-2014" title="Scaling Apache Storm - Strata + Hadoop World 2014" target="_blank">Scaling Apache Storm - Strata + Hadoop World 2014</a> </strong> from <strong><a href="//www.slideshare.net/ptgoetz" target="_blank">P. Taylor Goetz</a></strong> </div>
						</div>

						<div class="col-md-6 brick">
							<h2>Yahoo compares Storm and Spark
								<span>
									from Chicago Hadoop Users Group
								</span>
							</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/BRgWgMTzazVSbG" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/ChicagoHUG/yahoo-compares-storm-and-spark" title="Yahoo compares Storm and Spark" target="_blank">Yahoo compares Storm and Spark</a> </strong> from <strong><a href="//www.slideshare.net/ChicagoHUG" target="_blank">Chicago Hadoop Users Group</a></strong> </div>
						</div>
					</div>

					<div class="row" style="padding-left: 45px;">
						<div class="col-md-6 brick">
							<h2>Hadoop Summit Europe 2014: Apache Storm Architecture
								<span>
									from P. Taylor Goetz
								</span>
							</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/m9vKPotXvQ8hb7" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/ptgoetz/storm-hadoop-summit2014" title="Hadoop Summit Europe 2014: Apache Storm Architecture" target="_blank">Hadoop Summit Europe 2014: Apache Storm Architecture</a> </strong> from <strong><a href="//www.slideshare.net/ptgoetz" target="_blank">P. Taylor Goetz</a></strong> </div>
						</div>

						<div class="col-md-6 brick">
							<h2>Storm: distributed and fault-tolerant realtime computation
								<span>
									from nathanmarz
								</span>
							</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/zF8J7y8oz4Qtbc" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/nathanmarz/storm-distributed-and-faulttolerant-realtime-computation" title="Storm: distributed and fault-tolerant realtime computation" target="_blank">Storm: distributed and fault-tolerant realtime computation</a> </strong> from <strong><a href="//www.slideshare.net/nathanmarz" target="_blank">nathanmarz</a></strong> </div>
						</div>

						<div class="col-md-6 brick">
							<h2>Realtime Analytics with Storm and Hadoop
								<span>
									from Hadoop Summit
								</span>
							</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/wAvMj9LtK7OAwn" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/Hadoop_Summit/realtime-analytics-with-storm" title="Realtime Analytics with Storm and Hadoop" target="_blank">Realtime Analytics with Storm and Hadoop</a> </strong> from <strong><a href="//www.slideshare.net/Hadoop_Summit" target="_blank">Hadoop Summit</a></strong> </div>
						</div>
					</div>
				</div>
		    </div>
		</div>
    </div>
</div>


