---
layout: documentation
title: Maven
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Building Storm with Maven</h1>
    <p>To develop topologies, you'll need the Storm jars on your classpath. You should either include the unpacked jars in the classpath for your project or use Maven to include Storm as a development dependency. Storm is hosted on Maven Central. To include Storm in your project as a development dependency, add the following to your pom.xml:</p>
<div class="highlight"><pre><code class="language-xml" data-lang="xml"><span class="nt">&lt;dependency&gt;</span>
  <span class="nt">&lt;groupId&gt;</span>org.apache.storm<span class="nt">&lt;/groupId&gt;</span>
  <span class="nt">&lt;artifactId&gt;</span>storm-core<span class="nt">&lt;/artifactId&gt;</span>
  <span class="nt">&lt;version&gt;</span>0.9.3<span class="nt">&lt;/version&gt;</span>
  <span class="nt">&lt;scope&gt;</span>provided<span class="nt">&lt;/scope&gt;</span>
<span class="nt">&lt;/dependency&gt;</span>
</code></pre></div>
<p><a href="https://github.com/apache/storm/blob/master/examples/storm-starter/pom.xml" target="_blank">Here's an example</a> of a pom.xml for a Storm project.</p>

<h3>Developing Storm</h3>

<p>Please refer to <a href="https://github.com/apache/storm/blob/master/DEVELOPER.md" target="_blank">DEVELOPER.md</a> for more details.</p>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
<!--Footer Begin-->
<footer>
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-3">
            	<div class="footer-widget">
                	<h5>Meetups</h5>
                    <ul class="latest-news">
                        <li><a href="http://www.meetup.com/Apache-Storm-Apache-Kafka/">Sunnyvale, CA</a> <span class="small">(10 May 2015)</span></li>
                        <li><a href="http://www.meetup.com/Apache-Storm-Kafka-Users/">Seatle, WA</a> <span class="small">(27 Jun 2015)</span></li>                        
                    </ul> 
                </div>
            </div>
            <div class="col-md-3">
            	<div class="footer-widget">
                	<h5>About Storm</h5>
                    <p>Storm integrates with any queueing system and any database system. Storm's spout abstraction makes it easy to integrate a new queuing system. Likewise, integrating Storm with database systems is easy.</p>
                    <!--<div class="social">
                        <a href="#"><i class="fa fa-twitter twitter"></i></a>
                        <a href="#"><i class="fa fa-pinterest pinterest"></i></a>
                        <a href="#"><i class="fa fa-facebook facebook"></i></a>
                        <a href="#"><i class="fa fa-google-plus google-plus"></i></a>
                        <a href="#"><i class="fa fa-linkedin linkedin"></i></a>
                    </div>-->
                </div>
            </div>
            <div class="col-md-3">
            	<div class="footer-widget">
                	<h5>First Look</h5>
                    <ul class="footer-list">
                    	<li><a href="../rationale.html">Rationale</a></li>
                        <li><a href="../tutorial.html">Tutorial</a></li>
                        <li><a href="../setting-up.html">Setting up development environment</a></li>
                        <li><a href="../creating-project.html">Creating a new Storm project</a></li>
                    </ul>
                </div>
            </div>
            <div class="col-md-3">
            	<div class="footer-widget">
                	<h5>Documentation</h5>
                    <ul class="footer-list">
                    	<li><a href="../documentation-index.html">Index</a></li>
                        <li><a href="../documentation.html">Manual</a></li>
                        <li><a href="https://storm.apache.org/javadoc/apidocs/index.html">Javadoc</a></li>
                        <li><a href="../faq.html">FAQ</a></li>
                    </ul>
                </div>
            </div>
        </div>
        <hr/>
        <div class="row">	
        	<div class="col-md-12">
            	<p align="center">Copyright © 2014 <a href="http://www.apache.org">Apache Software Foundation</a>. All Rights Reserved. Apache Storm, Apache, the Apache feather logo, and the Apache Storm project logos are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.</p>
            </div>
        </div>
    </div>
</footer>
<!--Footer End-->
<!-- Scroll to top -->
<span class="totop"><a href="#"><i class="fa fa-angle-up"></i></a></span>
</body>
</html>
