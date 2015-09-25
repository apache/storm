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
                    
<!-- ################### -->
					<div class="brickSS">
						<div class="row">
						    <div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/_h5VjQoAJq4" frameborder="0" allowfullscreen></iframe>
						    </div>
							<div class="col-md-6">
						    	<h3>P. Taylor Goetz - Beyond the Tweeting Toaster</h3>
								<div>
									<p>Published on Sep 1, 2015</p><p>
            In this session we will look at how streaming sensor data fits into a variety of (I)IoT analytics use cases, and how Apache Storm and Kafka fit into an overall architecture for large-scale streaming analytics. You will also learn how to leverage the highly accessible Arduino microcontroller platform to create low-cost sensor networks and stream data to Apache Storm for analysis in real time. Finally, we will give a live demonstration of sensor analysis using Kafka, Storm, and an out-of-the-box Arduino board (no soldering required!).
			</p>
								</div>
						    </div>
						    
						</div>
					</div>

<!-- ################### -->
<!-- ################### -->
					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						    	<h3>David Wilcox and Bobby Evans - Yahoo Ad Manager Plus: A Case Study</h3>
								<div>
									<p>Published on Jun 30, 2015</p><p>
            The Yahoo Ad Manager Plus platform (YAM+) provides reports to advertisers on impressions, clicks, and conversions for their ad campaigns. Impressions and clicks are straightforward. Conversions require joining "action beacons" from advertisers with impressions and clicks from advertisements served by YAM+. A conversion is recorded if a user clicked on or was shown an advertisement associated with the campaign identified in the beacon. This presentation describes a storm topology that uses HBase and Druid to provide low-latency feedback to advertisers on the performance of their campaigns. It covers storm and HBase tuning that was needed to support this reporting at production scale.
			</p>
								</div>
						    </div>
						    <div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/h77pMHzFrSg" frameborder="0" allowfullscreen></iframe>
						    </div>
						</div>
					</div>

<!-- ################### -->                    

<!-- ################### -->
					<div class="brickSS">
						<div class="row">
						    <div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/pB9d3tMM__k" frameborder="0" allowfullscreen></iframe>
						    </div>
							<div class="col-md-6">
						    	<h3>Bobby Evans - From Gust to Tempest: Scaling Apache Storm</h3>
								<div>
									<p>Published on Jun 30, 2015</p><p>
            At Yahoo, we extensively use the Apache Storm distributed real-time computation platform at medium scale deployment. In this talk we overview a collection of recent developments at Yahoo enabling massive Storm scaling to an order of magnitude larger clusters. They include a resource aware scheduler, load-aware shuffle grouping, a stand-alone heart-beat server that reduces the load on ZooKeeper, compression of ZooKeeper data and using timestamps instead of whole data through ZooKeeper, and finally a new distributed cache mechanism to distribute large files required by bolts.
			</p>
								</div>
						    </div>
						    
						</div>
					</div>

<!-- ################### -->                    
<!-- ################### -->
					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						    	<h3>Sheetal Dolas - Design Patterns for real time data analytics</h3>
								<div>
									<p>Published on Jun 30, 2015</p><p>
            Businesses are moving from large-scale batch data analysis to large-scale real-time data analysis. Apache Storm has emerged as one of the most popular platforms for the purpose.</p><p>
            This talk covers proven design patterns for real time stream processing. Patterns that have been vetted in large-scale production deployments that process 10s of billions of events/day and 10s of terabytes of data/day.
			</p>
								</div>
						    </div>
						    <div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/hiBc6bHoe3o" frameborder="0" allowfullscreen></iframe>
						    </div>
						</div>
					</div>

<!-- ################### -->
                    
<!-- ################### -->
					<div class="brickSS">
						<div class="row">
						    <div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/ja4Qj9-l6WQ" frameborder="0" allowfullscreen></iframe>
						    </div>
							<div class="col-md-6">
						    	<h3>Andrew Montalenti - streamparse: real-time streams with Python and Apache Storm - PyCon 2015</h3>
								<div>
									<p>Published on Apr 12, 2015</p><p>
			Real-time streams are everywhere, but does Python have a good way of processing them? Until recently, there were no good options. A new open source project, streamparse, makes working with real-time data streams easy for Pythonistas. If you have ever wondered how to process 10,000 data tuples per second with Python -- while maintaining high availability and low latency -- this talk is for you.</p>
								</div>
						    </div>
						    
						</div>
					</div>

<!-- ################### -->
<!-- ################### -->
					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						    	<h3>P. Taylor Goetz - Scaling Storm: Cluster Sizing and Performance Optimization</h3>
								<div>
									<p>Published on Jun 23, 2014</p><p>
			ne of the most commonly asked questions about Storm is how to properly size and scale a cluster for a given use case. While there is no magic bullet when it comes to capacity planning for a Storm cluster, there are many operational and development techniques that can be applied to eek out the maximum throughput for a given application. In this session we’ll cover capacity planning, performance tuning and optimization from both an operational and development perspective. We will discuss the basics of scaling, common mistakes and misconceptions, how different technology decisions affect performance, and how to identify and scale around the bottlenecks in a Storm deployment.</p>
								</div>
						    </div>
						    <div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/cH8hKyf4Y40" frameborder="0" allowfullscreen></iframe>
						    </div>
						</div>
					</div>

<!-- ################### -->

					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/hVO5nbxnBkU" frameborder="0" allowfullscreen></iframe>
						    </div>
						    <div class="col-md-6">
						        <h3>Real-Time Big Data Analytics with Storm
								</h3>
						        <div>
						        	<p>Published on Oct 12, 2013</p><p>
			This talk provides an overview of the open source Storm system for processing Big Data in realtime. The talk starts with an overview of the technology, including key components: Nimbus, Zookeeper, Topology, Tuple, Trident. The presentation then dives into the complex Big Data architecture in which Storm can be integrated. The result is a compelling stack of technologies including integrated Hadoop clusters, MPP, and NoSQL databases.

			The presentation then reviews real world use cases for realtime Big Data analytics. Social updates, in particular real-time news feeds on sites like Twitter and Facebook, benefit from Storm's capacity to process benefits from distributed logic of streaming. Another case study is financial compliance monitoring, where Storm plays a primary role in reducing the market data to a useable subset in realtime. In a final use case, Storm is crucial to collect rich operational intelligence, because it builds multidimensional stats and executes distributed queries.</p>
						        </div>
							</div>
						</div>
					</div>

<!-- ################### -->

                    
					<div class="brickSS">
						<div class="row">
							
						    <div class="col-md-6">
						        <h3>Andrew Montalenti & Keith Bourgoin - Real-time streams and logs with Storm and Kafka - PyData SV 2014 
								</h3>
						        <div>
						        	<p>Published on Jun 12, 2014</p><p>
			
			Some of the biggest issues at the center of analyzing large amounts of data are query flexibility, latency, and fault tolerance. Modern technologies that build upon the success of "big data" platforms, such as Apache Hadoop, have made it possible to spread the load of data analysis to commodity machines, but these analyses can still take hours to run and do not respond well to rapidly-changing data sets.</p>

			<p>A new generation of data processing platforms -- which we call "stream architectures" -- have converted data sources into streams of data that can be processed and analyzed in real-time. This has led to the development of various distributed real-time computation frameworks (e.g. Apache Storm) and multi-consumer data integration technologies (e.g. Apache Kafka). Together, they offer a way to do predictable computation on real-time data streams.</p>

			<p>In this talk, we will give an overview of these technologies and how they fit into the Python ecosystem. This will include a discussion of current open source interoperability options with Python, and how to combine real-time computation with batch logic written for Hadoop. We will also discuss Kafka and Storm's alternatives, current industry usage, and some real-world examples of how these technologies are being used in production by Parse.ly today.</p>
						        </div>
							</div>
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/od8U-XijzlQ" frameborder="0" allowfullscreen></iframe>
						    </div>
						</div>
					</div>
<!-- ################### -->
					<div class="brickSS">
						<div class="row">
                            
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/uJ5rdAPHE1w" frameborder="0" allowfullscreen></iframe>
						    </div>
							<div class="col-md-6">
						        <h3>Yahoo talks about Spark vs. Storm
								</h3>
						        <div>
						        	<p>Published on Sep 18, 2014</p><p>
			Bobby Evans and Tom Graves, the engineering leads for Spark and Storm development at Yahoo will talk about how these technologies are used on Yahoo's grids and reasons why to use one or the other.</p>

			<p>Bobby Evans is the low latency data processing architect at Yahoo. He is a PMC member on many Apache projects including Storm, Hadoop, Spark, and Tez. His team is responsible for delivering Storm as a service to all of Yahoo and maintaining Spark on Yarn for Yahoo (Although Tom really does most of that work).</p>

			<p>Tom Graves a Senior Software Engineer on the Platform team at Yahoo. He is an Apache PMC member on Hadoop, Spark, and Tez. His team is responsible for delivering and maintaining Spark on Yarn for Yahoo.</p>
						        </div>
							</div>
							
						    
						</div>
					</div>
<!-- ################### -->
					<div class="brickSS">

						    <div class="col-md-6">
						        <h3>Apache Storm Deployment and Use Cases by Spotify Developers
								</h3>
						        <div>
						        	<p>Published on Apr 3, 2014</p><p>
			This talk was presented at the New York City Storm User Group hosted by Spotify on March 25, 2014.</p><p>
			This is the first time that a Spotify engineer has spoken publicly about their deployment and use cases for Storm! In this talk, Software Engineer Neville Li describes:
            <ul><li>
			Real-time features developed using Storm and Kafka including recommendations, social features, data visualization and ad targeting</li>

			<li>Architecture</li>

			<li>Production integration</li>

			<li>Best practices for deployment</li></ul></p>
            <p>Spotify is an exciting case study - users create 600 Gigabyte of data per day and 150 Gigabyte of data per day via different services. Every day 4 Terabyte of data is generated in Hadoop, a 700-node cluster running over 2.000 jobs per day. They currently have 28 Petabytes of storage, spread out over 4 data centres across the world.</p>
						        </div>
							</div>
    						<div class="row">
    							<div class="col-md-6">
    						        <iframe width="560" height="315" src="https://www.youtube.com/embed/5F0eQ7mkpTU" frameborder="0" allowfullscreen></iframe>
    						    </div>
						</div>
					</div>
                    
<!-- ################### -->
<!-- ################### -->
					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/CrABmVi12_A" frameborder="0" allowfullscreen></iframe>
						    </div>
							<div class="col-md-6">
						        <h3>Nathan Bijnens: A Real-Time Architecture Using Hadoop &amp; Storm</h3>
						        <div>
                    									<p>Published on Dec 19, 2013</p>
                                <p>With the proliferation of data sources and growing user bases, the amount of data generated requires new ways for storage and processing. Hadoop opened new possibilities, yet it falls short of instant delivery. Adding stream processing using Nathan Marz's Storm, can overcome this delay and bridge the gap to real-time aggregation and reporting. On the Batch layer all master data is kept and is immutable. Once the base data is stored a recurring process will index the data. This process reads all master data, parses it and will create new views out of it. The new views will replace all previously created views. In the Speed layer data is stored not yet absorbed in the Batch layer. Hours of data instead of years of data. Once the data is indexed in the Batch layer the data can discarded in the Speed layer. The Query Service merges the data from the Speed and Batch layers. This talk focuses on the Lambda architecture, which combines multiple technologies to be able to process vast amounts of data, while still being able to react timely and report near real-time statistics. Filmed at JAX London 2013.</p>
						        </div>
							</div>
							
						    
						</div>
					</div>
<!-- ################### -->

					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <h3>Infrastructure at Scale: Apache Kafka, Twitter Storm & Elastic Search</h3>
						        <div>
                    									<p>Published on Nov 29, 2013</p><p>
                    				This is a technical architect's case study of how Loggly has employed the latest social-media-scale technologies as the backbone ingestion processing for our multi-tenant, geo-distributed, and real-time log management system. This presentation describes design details of how we built a second-generation system fully leveraging AWS services including Amazon Route 53 DNS with heartbeat and latency-based routing, multi-region VPCs, Elastic Load Balancing, Amazon Relational Database Service, and a number of pro-active and re-active approaches to scaling computational and indexing capacity.</p>
                                    <p>
                    				The talk includes lessons learned in our first generation release, validated by thousands of customers; speed bumps and the mistakes we made along the way; various data models and architectures previously considered; and success at scale: speeds, feeds, and an unmeltable log processing engine.</p>
						        </div>
							</div>
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/LpNbjXFPyZ0" frameborder="0" allowfullscreen></iframe>
						    </div>
						    
						</div>
					</div>
<!-- ################### -->

<!-- ################### -->

					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/hVO5nbxnBkU" frameborder="0" allowfullscreen></iframe>
						    </div>
						    <div class="col-md-6">
						        <h3>Real-Time Big Data Analytics with Storm
								</h3>
						        <div>
						        	<p>Published on Oct 12, 2013</p><p>
			This talk provides an overview of the open source Storm system for processing Big Data in realtime. The talk starts with an overview of the technology, including key components: Nimbus, Zookeeper, Topology, Tuple, Trident. The presentation then dives into the complex Big Data architecture in which Storm can be integrated. The result is a compelling stack of technologies including integrated Hadoop clusters, MPP, and NoSQL databases.</p>
            <p>
			The presentation then reviews real world use cases for realtime Big Data analytics. Social updates, in particular real-time news feeds on sites like Twitter and Facebook, benefit from Storm's capacity to process benefits from distributed logic of streaming. Another case study is financial compliance monitoring, where Storm plays a primary role in reducing the market data to a useable subset in realtime. In a final use case, Storm is crucial to collect rich operational intelligence, because it builds multidimensional stats and executes distributed queries.</p>
						        </div>
							</div>
						</div>
					</div>

<!-- ################### -->

                    
<!-- ################### -->
					<div class="brickSS">
						<div class="row">
							<div class="col-md-6">
						        <h3>ETE 2012: Nathan Marz on Storm
								</h3>
						        <div>
						        	<p>Published on May 15, 2012</p><p>
            
            Storm makes it easy to write and scale complex realtime computations on a cluster of computers, doing for realtime processing what Hadoop did for batch processing. Storm guarantees that every message will be processed. And it's fast -- you can process millions of messages per second with a small cluster. Best of all, you can write Storm topologies using any programming language. Storm was open-sourced by Twitter in September of 2011 and has since been adopted by numerous companies around the world.</p><p>
            Storm provides a small set of simple, easy to understand primitives. These primitives can be used to solve a stunning number of realtime computation problems, from stream processing to continuous computation to distributed RPC. In this talk you'll learn:
            <ul>
            <li>The concepts of Storm: streams, spouts, bolts, and topologies</li>
            <li>Developing and testing topologies using Storm's local mode</li>
            <li>Deploying topologies on Storm clusters</li>
            <li>How Storm achieves fault-tolerance and guarantees data processing</li>
            <li>Computing intense functions on the fly in parallel using Distributed RPC</li>
            <li>Making realtime computations idempotent using transactional topologies</li>
            <li>Examples of production usage of Storm</li></ul></p>
						        </div>
							</div>
							<div class="col-md-6">
						        <iframe width="560" height="315" src="https://www.youtube.com/embed/bdps8tE0gYo" frameborder="0" allowfullscreen></iframe>
						    </div>
						    
						</div>
					</div>
<!-- ################### -->
<!-- ########## END VIDEOS ######### -->
				</div>


				<div role="tabpanel" class="tab-pane" id="slideshows">
					<div class="row" style="padding-left: 45px;">
						<div class="col-md-6 brick">
							<h2>Apache Storm 0.9 basic training</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/GUD3Y58U1f973x" width="425" height="355" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/miguno/apache-storm-09-basic-training-verisign" title="Apache Storm 0.9 basic training - Verisign" target="_blank">Apache Storm 0.9 basic training - Verisign</a> </strong> from <strong><a href="//www.slideshare.net/miguno" target="_blank">Michael Noll</a></strong> </div>
						</div>

						<div class="col-md-6 brick">
							<h2>Scaling Apache Storm - Strata + Hadoop World 2014</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/NRMYq1985xMCWv" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/ptgoetz/scaling-apache-storm-strata-hadoopworld-2014" title="Scaling Apache Storm - Strata + Hadoop World 2014" target="_blank">Scaling Apache Storm - Strata + Hadoop World 2014</a> </strong> from <strong><a href="//www.slideshare.net/ptgoetz" target="_blank">P. Taylor Goetz</a></strong> </div>
						</div>

						<div class="col-md-6 brick">
							<h2>Yahoo compares Storm and Spark</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/BRgWgMTzazVSbG" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/ChicagoHUG/yahoo-compares-storm-and-spark" title="Yahoo compares Storm and Spark" target="_blank">Yahoo compares Storm and Spark</a> </strong> from <strong><a href="//www.slideshare.net/ChicagoHUG" target="_blank">Chicago Hadoop Users Group</a></strong> </div>
						</div>
					</div>

					<div class="row" style="padding-left: 45px;">
						<div class="col-md-6 brick">
							<h2>Hadoop Summit Europe 2014: Apache Storm Architecture</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/m9vKPotXvQ8hb7" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/ptgoetz/storm-hadoop-summit2014" title="Hadoop Summit Europe 2014: Apache Storm Architecture" target="_blank">Hadoop Summit Europe 2014: Apache Storm Architecture</a> </strong> from <strong><a href="//www.slideshare.net/ptgoetz" target="_blank">P. Taylor Goetz</a></strong> </div>
						</div>

						<div class="col-md-6 brick">
							<h2>Storm: distributed and fault-tolerant realtime computation</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/zF8J7y8oz4Qtbc" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/nathanmarz/storm-distributed-and-faulttolerant-realtime-computation" title="Storm: distributed and fault-tolerant realtime computation" target="_blank">Storm: distributed and fault-tolerant realtime computation</a> </strong> from <strong><a href="//www.slideshare.net/nathanmarz" target="_blank">Nathan Marz</a></strong> </div>
						</div>

						<div class="col-md-6 brick">
							<h2>Realtime Analytics with Storm and Hadoop</h2>
							<iframe src="//www.slideshare.net/slideshow/embed_code/key/wAvMj9LtK7OAwn" width="340" height="290" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/Hadoop_Summit/realtime-analytics-with-storm" title="Realtime Analytics with Storm and Hadoop" target="_blank">Realtime Analytics with Storm and Hadoop</a> </strong> from <strong><a href="//www.slideshare.net/Hadoop_Summit" target="_blank">Hadoop Summit</a></strong> </div>
						</div>
					</div>
				</div>
		    </div>
		</div>
    </div>
</div>


