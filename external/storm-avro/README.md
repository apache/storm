#Storm Avro

Storm integration for [Apache Avro](http://avro.apache.org/).

## GenericAvroSerializer & FixedAvroSerializer & ConfluentAvroSerializer
These Serializers are the implementations of `AbstractAvroSerializer`.
To serialize Avro GenericRecord between worker and another worker you **must** register the appropriate Kryo serializers with your topology configuration.  A convenience
method is provided for this:

`AvroUtils.addAvroKryoSerializations(conf);`

By default Storm will use the `GenericAvroSerializer` to handle serialization. This will work, but there are much faster options available if you can pre-define the schemas you will be using or utilize an external schema registry. An implementation using the Confluent Schema Registry is provided, but others can be implemented and provided to Storm.
The configuration property is `Config.TOPOLOGY_AVRO_SERIALIZER`.

Please see the javadoc for classes in org.apache.storm.avro for information about using the built-in options or creating your own.


### AvroSchemaRegistry

```java
public interface AvroSchemaRegistry extends Serializable {
    String getFingerprint(Schema schema);

    Schema getSchema(String fingerPrint);
}
```

### AbstractAvroSerializer

```java
public abstract class AbstractAvroSerializer extends Serializer<GenericContainer> implements AvroSchemaRegistry {

    @Override
    public void write(Kryo kryo, Output output, GenericContainer record) { }

    @Override
    public GenericContainer read(Kryo kryo, Input input, Class<GenericContainer> aClass) { }
}
```

## DirectAvroSerializer

`DirectAvroSerializer` provide the ability to serialize Avro `GenericContainer` directly. 

```java
public interface DirectAvroSerializer extends Serializable {

    public byte[] serialize(GenericContainer record) throws IOException;

    public GenericContainer deserialize(byte[] bytes, Schema schema) throws IOException;

}
```

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

 * Aaron Niskode-Dossett ([dossett@gmail.com](mailto:dossett@gmail.com))
 
