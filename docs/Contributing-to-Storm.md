---
title: Contributing
layout: documentation
documentation: true
---

### Getting started with contributing

Some of the issues on the [issue tracker](https://github.com/apache/storm/issues) are marked with the ["good first issue"](https://github.com/apache/storm/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22) label. If you're interested in contributing to Storm but don't know where to begin, these are good issues to start with. These issues are a great way to get your feet wet with learning the codebase because they require learning about only an isolated portion of the codebase and are a relatively small amount of work.

### Learning the codebase

The [Implementation docs](Implementation-docs.html) section of the wiki gives detailed walkthroughs of the codebase. Reading through these docs is highly recommended to understand the codebase.

### Contribution process

Contributions to the Storm codebase should be sent as [GitHub](https://github.com/apache/storm) pull requests. Use a short descriptive title, and if the pull request is related to an existing issue, mention it in the description (e.g. `Fixes #123`). If there's any problems to the pull request we can iterate on it using GitHub's commenting features.

For small patches, feel free to submit pull requests directly for them. For larger contributions, please use the following process. The idea behind this process is to prevent any wasted work and catch design issues early on:

1. Open an issue on the [issue tracker](https://github.com/apache/storm/issues) if one doesn't exist already
2. Comment on the issue with your plan for implementing the issue. Explain what pieces of the codebase you're going to touch and how everything is going to fit together.
3. Storm committers will iterate with you on the design to make sure you're on the right track
4. Implement your issue, submit a pull request, and iterate from there.

### Modules built on top of Storm

Modules built on top of Storm (like spouts, bolts, etc) that aren't appropriate for Storm core can be done as your own project or as part of [@stormprocessor](https://github.com/stormprocessor). To be part of @stormprocessor put your project on your own Github and then send an email to the mailing list proposing to make it part of @stormprocessor. Then the community can discuss whether it's useful enough to be part of @stormprocessor. Then you'll be added to the @stormprocessor organization and can maintain your project there. The advantage of hosting your module in @stormprocessor is that it will be easier for potential users to find your project.

### Contributing documentation

Documentation contributions are very welcome! You can contribute documentation by pull request, using the same process as code contributions. Release specific documentation lives in `docs/` in the [Storm repository](https://github.com/apache/storm). Documentation not specific to a release, such as announcements, lives in the [storm-site](https://github.com/apache/storm-site) repository.

