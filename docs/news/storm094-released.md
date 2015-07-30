---
layout: news
title: Storm 0.9.4 released
---
<!--Post Header-->
<h3 class="news-title">Storm 0.9.4 released</h3>
<div class="news-meta">
    <i class="fa fa-calendar"></i> Mar 25, 2015 <i class="fa fa-user"></i> P. Taylor Goetz
</div>
<!--Post Body-->
<p>The Apache Storm community is pleased to announce that version 0.9.4 has been released and is available from <a href="/downloads.html">the downloads page</a>.</p>
<p>This is a maintenance release that includes a number of important bug fixes that improve Storm's stability and fault tolerance. We encourage users of previous versions to upgrade to this latest release.</p>
<h4>Thanks</h4>
<p>Special thanks are due to all those who have contributed to Apache Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are much appreciated.</p>
<h4>Full Changelog</h4>
<ul>
	<li>STORM-559: ZkHosts in README should use 2181 as port.</li>
	<li>STORM-682: supervisor should handle worker state corruption gracefully.</li>
	<li>STORM-693: when kafka bolt fails to write tuple, it should report error instead of silently acking.</li>
	<li>STORM-329: fix cascading Storm failure by improving reconnection strategy and buffering messages</li>
	<li>STORM-130: Supervisor getting killed due to java.io.FileNotFoundException: File '../stormconf.ser' does not exist.</li>
</ul>