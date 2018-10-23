---
title: Local Mode
layout: documentation
documentation: true
---
Local mode simulates a Storm cluster in process and is useful for developing and testing topologies. Running topologies in local mode is similar to running topologies [on a cluster](Running-topologies-on-a-production-cluster.html).

To run a topology in local mode you have two options.  The most common option is to run your topology with `storm local` instead of `storm jar`

This will bring up a local simulated cluster and force all interactions with nimbus to go through the simulated cluster instead of going to a separate process. By default this will run the process for 20 seconds before tearing down the entire cluster.  You can override this by including a `--local-ttl` command line option which sets the number of seconds it should run for.

### Programmatic

If you want to do some automated testing but without actually launching a storm cluster you can use the same classes internally that `storm local` does.

To do this you first need to pull in the dependencies needed to access these classes.  For the java API you should depend on `storm-server` as a `test` dependency.

To create an in-process cluster, simply use the `LocalCluster` class. For example:

```java
import org.apache.storm.LocalCluster;

...

try (LocalCluster cluster = new LocalCluster()) {
    //Interact with the cluster...
}
```

You can then submit topologies using the `submitTopology` method on the `LocalCluster` object. Just like the corresponding method on [StormSubmitter](javadocs/org/apache/storm/StormSubmitter.html), `submitTopology` takes a name, a topology configuration, and the topology object. You can then kill a topology using the `killTopology` method which takes the topology name as an argument.

The `LocalCluster` is an `AutoCloseable` and will shut down when close is called. 

many of the Nimbus APIs are also available through the LocalCluster.

### DRPC

DRPC can be run in local mode as well. Here's how to run the above example in local mode:

```java
try (LocalDRPC drpc = new LocalDRPC();
     LocalCluster cluster = new LocalCluster();
     LocalTopology topo = cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc))) {

    System.out.println("Results for 'hello':" + drpc.execute("exclamation", "hello"));
}
```

First you create a `LocalDRPC` object. This object simulates a DRPC server in process, just like how `LocalCluster` simulates a Storm cluster in process. Then you create the `LocalCluster` to run the topology in local mode. `LinearDRPCTopologyBuilder` has separate methods for creating local topologies and remote topologies. In local mode the `LocalDRPC` object does not bind to any ports so the topology needs to know about the object to communicate with it. This is why `createLocalTopology` takes in the `LocalDRPC` object as input.

After launching the topology, you can do DRPC invocations using the `execute` method on `LocalDRPC`.

Because all of the objects used are instances of AutoCloseable when the try blocks scope ends the topology is killed, the cluster is shut down and the drpc server also shuts down.

### Clojure API

Storm also offers a clojure API for testing.

[This blog post](http://www.pixelmachine.org/2011/12/21/Testing-Storm-Topologies-Part-2.html) talk about this, but is a little out of date.  To get this functionality you need to include the `storm-clojure-test` dependency.  This will pull in a lot of storm itself that should not be packaged with your topology, sp please make sure it is a test dependency only,.

### Debugging your topology with an IDE

One of the great use cases for local mode is to be able to walk through the code execution of your bolts and spouts using an IDE.  You can do this on the command line by adding the `--java-debug` option followed by the parameter you would pass to jdwp. This makes it simple to launch the local cluster with `-agentlib:jdwp=` turned on.

When running from within an IDE itself you can modify your code run run withing a call to `LocalCluster.withLocalModeOverride`

```java
public static void main(final String args[]) {
    LocalCluster.withLocalModeOverride(() -> originalMain(args), 10);
}
```

Or you could also modify the IDE to run "org.apache.storm.LocalCluster" instead of your main class when launching, and pass in the name of the class as an argument to it.  This will also trigger local mode, and is what `storm local` does behind the scenes. 

### Common configurations for local mode

You can see a full list of configurations [here](javadocs/org/apache/storm/Config.html).

1. **Config.TOPOLOGY_MAX_TASK_PARALLELISM**: This config puts a ceiling on the number of threads spawned for a single component. Oftentimes production topologies have a lot of parallelism (hundreds of threads) which places unreasonable load when trying to test the topology in local mode. This config lets you easy control that parallelism.
2. **Config.TOPOLOGY_DEBUG**: When this is set to true, Storm will log a message every time a tuple is emitted from any spout or bolt. This is extremely useful for debugging.A

These, like all other configs, can be set on the command line when launching your topology with the `-c` flag.  The flag is of the form `-c <conf_name>=<JSON_VALUE>`  so to enable debugging when launching your topology in local mode you could run

```
storm local topology.jar <MY_MAIN_CLASS> -c topology.debug=true
``` 
