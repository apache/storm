## About Storm JMS
Storm JMS is a generic framework for integrating JMS messaging within the Storm framework.

The [Storm Rationale page](https://github.com/nathanmarz/storm/wiki/Rationale) explains what storm is and why it was built.

Storm-JMS allows you to inject data into Storm via a generic JMS spout, as well as consume data from Storm via a generic JMS bolt.

Both the JMS Spout and JMS Bolt are data agnostic. To use them, you provide a simple Java class that bridges the JMS and Storm APIs and encapsulates and domain-specific logic.

## Components

### JMS Spout
The JMS Spout component allows for data published to a JMS topic or queue to be consumed by a Storm topology.

A JMS Spout connects to a JMS Destination (topic or queue), and emits Storm "Tuple" objects based on the content of the JMS message received.


### JMS Bolt
The JMS Bolt component allows for data within a Storm topology to be published to a JMS destination (topic or queue).

A JMS Bolt connects to a JMS Destination, and publishes JMS Messages based on the Storm "Tuple" objects it receives.

## Project Location
Primary development of storm-cassandra will take place at: 
https://github.com/ptgoetz/storm-cassandra

Point/stable (non-SNAPSHOT) release souce code will be pushed to:
https://github.com/nathanmarz/storm-contrib

Maven artifacts for releases will be available on maven central.


## Documentation

Documentation and tutorials can be found on the [Storm-JMS wiki](http://github.com/ptgoetz/storm-jms/wiki).

## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

## Contributors

* P. Taylor Goetz ([@ptgoetz](http://twitter.com/ptgoetz))
