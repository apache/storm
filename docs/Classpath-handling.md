---
title: Classpath Handling
layout: documentation
documentation: true
---
### Storm is an Application Container

Storm provides an application container environment, a la Apache Tomcat, which creates potential for classpath conflicts between Storm and your application.  The most common way of using Storm involves submitting an "uber JAR" containing your application code with all of its dependencies bundled in, and then Storm distributes this JAR to Worker nodes.  Then Storm runs your application within a Storm process called a `Worker` -- thus the JVM's classpath contains the dependencies of your JAR as well as whatever dependencies the Worker itself has.  So careful handling of classpaths and dependencies is critical for the correct functioning of Storm.

### Adding Extra Dependencies to Classpath

You no longer *need* to bundle your dependencies into your topology and create an uber JAR, there are now facilities for separately handling your topology's dependencies.  Furthermore, there are facilities for adding external dependencies into the Storm daemons.

The `storm.py` launcher script allows you to include dependencies into the launched program's classpath via a few different mechanisms:

1. The `--jar` and `--artifacts` options for the `storm jar` command: allow inclusion of non-bundled dependencies with your topology; i.e., allowing specification of JARs that were not bundled into the topology uber-jar.  This is required when using the `storm sql` command, which constructs a topology automatically without needing you to write code and build a topology JAR.
2. The `${STORM_DIR}/extlib/` and `${STORM_DIR}/extlib-daemon/` directories can have dependencies added to them for inclusion of plugins & 3rd-party libraries into the Storm daemons (e.g., Nimbus, UI, Supervisor, etc. -- use `extlib-daemon/`) and other commands launched via the `storm.py` script, e.g., `storm sql` and `storm jar` (use `extlib`). Notably, this means that the Storm Worker process does not include the `extlib-daemon/` directory into its classpath.
3. The `STORM_EXT_CLASSPATH` and `STORM_EXT_CLASSPATH_DAEMON` environment variables provide a similar functionality as those directories, but allows the user to place their external dependencies in alternative locations.
 * There is a wrinkle here: because the Supervisor daemon launches the Worker process, if you want `STORM_EXT_CLASSPATH` to impact your Workers, you will need to specify the `STORM_EXT_CLASSPATH` for the Supervisor daemon.  That will allow the Supervisor to consult this environment variable as it constructs the classpath of the Worker processes.

#### Which Facility to Choose?

You might have noticed the overlap between the first mechanism and the others. If you consider the `--jar` / `--artifacts` option versus the `extlib/` / `STORM_EXT_CLASSPATH` it is not obvious which one you should choose for using dependencies with your Worker processes. i.e., both mechanisms allow including JARs to be used for running your Worker processes. Here is my understanding of the difference: `--jar` / `--artifacts` will result in the dependencies being used for running the `storm jar/sql` command, *and* the dependencies will be uploaded and available in the classpath of the topology's `Worker` processes. Whereas the use of `extlib/` / `STORM_EXT_CLASSPATH` requires you to have distributed your JAR dependencies out to all Worker nodes.  Another difference is that `extlib/` / `STORM_EXT_CLASSPATH` would impact all topologies, whereas `--jar` / `--artifacts` is a topology-specific option.

### Abbreviation of Classpaths and Process Commands

When the `storm.py` script launches a `java` command, it first constructs the classpath from the optional settings mentioned above, as well as including some default locations such as the `${STORM_DIR}/`, `${STORM_DIR}/lib/`, `${STORM_DIR}/extlib/` and `${STORM_DIR}/extlib-daemon/` directories.  In past releases, Storm would enumerate all JARs in those directories and then explicitly add all of those JARs into the `-cp` / `--classpath` argument to the launched `java` commands.  As such, the classpath would get so long that the `java` commands could breach the Linux Kernel process table limit of 4096 bytes for recording commands.  That led to truncated commands in `ps` output, making it hard to operate Storm clusters because you could not easily differentiate the processes nor easily see from `ps` which port a worker is listening to.

After Storm dropped support for Java 5, this classpath expansion was no longer necessary, because Java 6 supports classpath wildcards. Classpath wildcards allow you to specify a directory ending with a `*` element, such as `foo/bar/*`, and the JVM will automatically expand the classpath to include all `.jar` files in the wildcard directory.  As of [STORM-2191](https://issues.apache.org/jira/browse/STORM-2191) Storm just uses classpath wildcards instead of explicitly listing all JARs, thereby shortening all of the commands and making operating Storm clusters a bit easier.
