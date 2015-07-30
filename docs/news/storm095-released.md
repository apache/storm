---
layout: news
title: Storm 0.9.5 released
---
<!--Post Header-->
<h3 class="news-title">Storm 0.9.5 released</h3>
<div class="news-meta">
    <i class="fa fa-calendar"></i> Jun 4, 2015 <i class="fa fa-user"></i> P. Taylor Goetz
</div>
<!--Post Body-->
<p>The Apache Storm community is pleased to announce that version 0.9.5 has been released and is available from <a href="/downloads.html">the downloads page</a>.</p>
<p>This is a maintenance release that includes a number of important bug fixes that improve Storm's stability and fault tolerance. We encourage users of previous versions to upgrade to this latest release.</p>
<h4>Thanks</h4>
<p>Special thanks are due to all those who have contributed to Apache Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are much appreciated.</p>
<h4>Full Changelog</h4>
<ul>
    <li>STORM-790: Log "task is null" instead of letting worker die when task is null in transfer-fn</li>
    <li>STORM-796: Add support for "error" command in ShellSpout</li>
    <li>STORM-745: fix storm.cmd to evaluate 'shift' correctly with 'storm jar'</li>
    <li>STORM-130: Supervisor getting killed due to java.io.FileNotFoundException: File '../stormconf.ser' does not exist.</li>
</ul>