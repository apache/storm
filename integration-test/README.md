End to end storm integration tests
==================================

Running tests end-to-end
------------------------
Assumption:
A single version of storm binary zip such as `storm-dist/binary/target/apache-storm-2.0.0-SNAPSHOT.zip` is present
The following command will bring up a vagrant cluster.
```sh
cd integration-test/config
vagrant up
```
This automatically will run `integration-test/run-it.sh`.
This brings up a vagrant machine, with storm and zookeeper daemons.
And runs all the tests against it.

Running tests for development & debugging
=========================================
```vagrant up``` command is setup as a complete auto-pilot.
Following describes how we can run individual tests against this vagrant cluster or any other cluster.

Configs for running
-------------------
Configuration for the Storm cluster in Vagrant can be found in `integration-test/config/storm.yaml`. The configuration for the tests when running against Vagrant can be found in `src/test/resources/storm-conf/`, which is used if you run Maven with the `intellij` profile.
Outside Vagrant, you will need to configure your cluster as normal through your cluster's storm.yaml file. You can configure the tests by pointing to a storm.yaml by adding "-Dstorm.conf.file=/your/path/to/storm.yaml". This file needs to contain `nimbus.seeds` pointing to your Nimbus URL(s).

If you're running against a cluster outside Vagrant, the test needs access to Nimbus and the test must be able to hit the Logviewer URLs at port 8000 on each Supervisor. 

Running all tests manually
--------------------------
To run all tests:
```sh
mvn clean package -DskipTests && mvn test -DskipTests=false
```

To run a single test:
```sh
mvn clean package -DskipTests && mvn test -DskipTests=false -Dtest=SlidingWindowCountTest#testWindowCount
```

Running tests from IDE
----------------------
You might have to enable intellij profile to make your IDE happy.
Make sure that the following is run before tests are launched.
```sh
mvn package
```

Running tests with custom storm version
---------------------------------------
You can supply custom storm version using `-Dstorm.version=<storm-version>` property to all the maven commands.
```sh
mvn clean package -DskipTests -Dstorm.version=<storm-version>
mvn test -DskipTests=false -Dtest=DemoTest -Dstorm.version=<storm-version>
```

To find version of the storm that you are running run `storm version` command.

Code
----
Start off by looking at file [DemoTest.java](https://github.com/apache/storm/blob/master/integration-test/src/test/java/org/apache/storm/st/DemoTest.java).
