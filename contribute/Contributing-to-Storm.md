---
title: Contributing
layout: documentation
documentation: true
---

### Getting started with contributing

Some of the issues on the [issue tracker](https://issues.apache.org/jira/browse/STORM) are marked with the "Newbie" label. If you're interesting in contributing to Storm but don't know where to begin, these are good issues to start with. These issues are a great way to get your feet wet with learning the codebase because they require learning about only an isolated portion of the codebase and are a relatively small amount of work.

### Learning the codebase

The [Implementation docs](/documentation/Implementation-docs.html) section of the wiki gives detailed walkthroughs of the codebase. Reading through these docs is highly recommended to understand the codebase.

### Contribution process

Contributions to the Storm codebase should be sent as [GitHub](https://github.com/apache/storm) pull requests. If there's any problems to the pull request we can iterate on it using GitHub's commenting features.

For small patches, feel free to submit pull requests directly for them. For larger contributions, please use the following process. The idea behind this process is to prevent any wasted work and catch design issues early on:

1. Open an issue on the [JIRA issue tracker](https://issues.apache.org/jira/browse/STORM) if one doesn't exist already
2. Comment on the issue with your plan for implementing the issue. Explain what pieces of the codebase you're going to touch and how everything is going to fit together.
3. Storm committers will iterate with you on the design to make sure you're on the right track
4. Read through the developer documentation on how to build, code style, testing, etc [DEVELOPER.md](https://github.com/apache/storm/blob/master/DEVELOPER.md) 
5. Implement your issue, submit a pull request prefixed with the JIRA ID (e.g. "STORM-123: add new feature foo"), and iterate from there.

### Contributing documentation

Documentation contributions are very welcome! The best way to send contributions is as emails through the mailing list.

