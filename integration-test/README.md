End to end storm-integration-tests
==================================

Bring up a cluster
------------------
Vagrant setup can be used for bringing up unsecure Storm 1.0
https://github.com/raghavgautam/storm-vagrant

Configs for running 
-------------------
Change following storm.yaml:
storm-integration-test/src/test/resources/storm.yaml

Running tests end to end from Commandline
-----------------------------------------
To run all tests:
```sh
mvn clean package -DskipTests && mvn test
```

To run a single test:
```sh
mvn clean package -DskipTests && mvn test -Dtest=SlidingWindowCountTest
```

Running tests from IDE
----------------------
Make sure that the following is run before tests are launched.
```sh
mvn package -DskipTests
```

Running tests with custom storm version
---------------------------------------
You can supply custom storm version using `-Dstorm.version=<storm-version>` property to all the maven commands.
```sh
mvn clean package -DskipTests -Dstorm.version=<storm-version>
mvn test -Dtest=DemoTest -Dstorm.version=<storm-version>
```

To find version of the storm that you are running run `storm version` command.

Submiting topologies
--------------------
All the topologies can be submitted using `storm jar` command.

Code
----
Start off by looking at file [DemoTest.java](https://github.com/raghavgautam/storm-integration-test/blob/master/src/test/java/com/hortonworks/storm/st/DemoTest.java).
