#Storm Kafka Monitor 

Tool to query kafka spout lags and show in Storm UI

## Usage
This tool provides a way to query kafka offsets that the spout has consumed successfully and the latest
offsets in kafka. It provides an easy way to see how the topology is performing. It is a command line
interface called storm-kafka-monitor in the bin directory. The results have also been included in storm
ui on the topology page. It supports both new and the old kafka spout. Please execute the command
line without any options to see usage.

```java
$STORM_HOME_DIR/bin/storm-kafka-monitor
```

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
 
