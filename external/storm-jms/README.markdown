## About Storm JMS
Storm JMS is a generic framework for integrating JMS messaging within the Storm framework.


Storm-JMS allows you to inject data into Storm via a generic JMS spout, as well as consume data from Storm via a generic JMS bolt.

Both the JMS Spout and JMS Bolt are data agnostic. To use them, you provide a simple Java class that bridges the JMS and Storm APIs and encapsulates and domain-specific logic.

## Components

### JMS Spout
The JMS Spout component allows for data published to a JMS topic or queue to be consumed by a Storm topology.

A JMS Spout connects to a JMS Destination (topic or queue), and emits Storm "Tuple" objects based on the content of the JMS message received.


### JMS Bolt
The JMS Bolt component allows for data within a Storm topology to be published to a JMS destination (topic or queue).

A JMS Bolt connects to a JMS Destination, and publishes JMS Messages based on the Storm "Tuple" objects it receives.


## Committer Sponsors

* P. Taylor Goetz ([@ptgoetz](http://twitter.com/ptgoetz))
