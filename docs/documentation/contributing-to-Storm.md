---
layout: default
title: Contributing to storm
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
        <div class="row">
            <div class="col-md-12">
                <h1 class="page-title">Contributing</h1>
                    
                    <h3 id="getting-started-with-contributing">Getting started with contributing</h3>

                    <p>Some of the issues on the <a href="https://issues.apache.org/jira/browse/STORM">issue tracker</a> are marked with the "Newbie" label. If you're interesting in contributing to Storm but don't know where to begin, these are good issues to start with. These issues are a great way to get your feet wet with learning the codebase because they require learning about only an isolated portion of the codebase and are a relatively small amount of work.</p>

                    <h3 id="learning-the-codebase">Learning the codebase</h3>

                    <p>The <a href="Implementation-docs.html">Implementation docs</a> section of the wiki gives detailed walkthroughs of the codebase. Reading through these docs is highly recommended to understand the codebase.</p>

                    <h3 id="contribution-process">Contribution process</h3>

                    <p>Contributions to the Storm codebase should be sent as <a href="https://github.com/apache/storm">GitHub</a> pull requests. If there's any problems to the pull request we can iterate on it using GitHub's commenting features.</p>

                    <p>For small patches, feel free to submit pull requests directly for them. For larger contributions, please use the following process. The idea behind this process is to prevent any wasted work and catch design issues early on:</p>

                    <ol>
                    <li>Open an issue on the <a href="https://issues.apache.org/jira/browse/STORM">issue tracker</a> if one doesn't exist already</li>
                    <li>Comment on the issue with your plan for implementing the issue. Explain what pieces of the codebase you're going to touch and how everything is going to fit together.</li>
                    <li>Storm committers will iterate with you on the design to make sure you're on the right track</li>
                    <li>Implement your issue, submit a pull request, and iterate from there.</li>
                    </ol>

                    <h3 id="modules-built-on-top-of-storm">Modules built on top of Storm</h3>

                    <p>Modules built on top of Storm (like spouts, bolts, etc) that aren't appropriate for Storm core can be done as your own project or as part of <a href="https://github.com/stormprocessor">@stormprocessor</a>. To be part of @stormprocessor put your project on your own Github and then send an email to the mailing list proposing to make it part of @stormprocessor. Then the community can discuss whether it's useful enough to be part of @stormprocessor. Then you'll be added to the @stormprocessor organization and can maintain your project there. The advantage of hosting your module in @stormprocessor is that it will be easier for potential users to find your project.</p>

                    <h3 id="contributing-documentation">Contributing documentation</h3>

                    <p>Documentation contributions are very welcome! The best way to send contributions is as emails through the mailing list.</p>

  
            </div>
        </div>
    </div>
</div>
<!--Content End-->