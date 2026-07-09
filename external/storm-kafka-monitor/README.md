# Storm Kafka Monitor 

Tool to query kafka spout lags and show in Storm UI

## Installation

The storm-kafka-monitor jars (and their Kafka client dependencies) are only
needed to display Kafka spout lag in the UI or to run the `storm-kafka-monitor`
command, so they are bundled only in the full distribution:

| Distribution | storm-kafka-monitor jars |
|---|---|
| `apache-storm-x.x.x.tar.gz` (full) | bundled under `lib-tools/storm-kafka-monitor`, works out of the box |
| `apache-storm-x.x.x-lite.tar.gz` (lite) | not bundled, README only |

The Storm UI degrades gracefully when they are absent (no lag is shown and a
hint is logged once).

With the **lite** distribution, install the jars on the UI host with the helper
script, then restart the UI:

```bash
$STORM_HOME/bin/storm-kafka-monitor-fetch
```

It resolves `org.apache.storm:storm-kafka-monitor` and its runtime dependencies
from Maven Central into `lib-tools/storm-kafka-monitor`. Useful options:

```bash
bin/storm-kafka-monitor-fetch --version 3.0.0
# pass extra arguments through to Maven (internal mirror / offline repo)
bin/storm-kafka-monitor-fetch -- -s /etc/maven/settings.xml
```

## Usage
This tool provides a way to query kafka offsets that the spout has consumed successfully and the latest
offsets in kafka. It provides an easy way to see how the topology is performing. It is a command line
interface called storm-kafka-monitor in the bin directory. The results have also been included in storm
ui on the topology page. It supports both new and the old kafka spout. Please execute the command
line without any options to see usage.

```java
$STORM_HOME_DIR/bin/storm-kafka-monitor
```
This script runs ```org.apache.storm.kafka.monitor.KafkaOffsetLagUtil``` with the passed in
parameters. The following parameters are supported:

- ```required``` -t or --topics \<comma-separated-topics>
- ```required``` -b or --bootstrap-brokers \<brokers>
- ```required``` -g or --groupid \<groupid>
- ```optional``` -s or --security-protocol \<security-protocol>
- ```optional``` -c or --consumer-config \<config-properties-file>

## Future Work 
The offset lag calculation support for trident kafka spouts will be added soon.

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

## Committer Sponsors

 * Sriharsha Chintalapani ([sriharsha@apache.org](mailto:sriharsha@apache.org))
 
