# Base classes for developing Apache Storm multilang components using Node.js

## Installation

```bash
$ npm install -g storm-multilang-js
```

## Basic Usage

Extend the BasicBolt class

```javascript
const BasicBolt = require('storm-multilang-js').BasicBolt;

class MyBasicBolt extends BasicBolt {
    
    process(tuple, done) {
        // do something with tuple.values
        // call this.emit() to pass data to a downstream Bolt
        done();
    }
}

module.exports = MyBasicBolt
```

Invoke from Storm using the stormjs utility `stormjs ./mybasicbolt.js`

## Deploying

Deploying topologies with Node.js spouts and bolts is very easy using [Flux](http://storm.apache.org/releases/${project.version}/flux.html).

See [Using non JVM languages with Storm](http://storm.apache.org/releases/${project.version}/Using-non-JVM-languages-with-Storm.html) for additional information.

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
