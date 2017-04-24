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
```vagrant up``` command is steup as a complete auto-pilot.
Following describes how we can run individual tests against this vagrant cluster or any other cluster.

Configs for running
-------------------
The supplied configuration will run tests against vagrant setup. However, it can be changed to use a different cluster.
Change `integration-test/src/test/resources/storm.yaml` as necessary.

Running all tests manually
--------------------------
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
You might have to enable intellij profile to make your IDE happy.
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

Code
----
Start off by looking at file [DemoTest.java](https://github.com/apache/storm/blob/master/integration-test/src/test/java/org/apache/storm/st/DemoTest.java).
