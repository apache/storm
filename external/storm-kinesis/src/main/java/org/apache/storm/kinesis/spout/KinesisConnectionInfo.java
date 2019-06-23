/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kinesis.spout;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Arrays;

import org.objenesis.strategy.StdInstantiatorStrategy;

public class KinesisConnectionInfo implements Serializable {
    private final byte[] serializedKinesisCredsProvider;
    private final byte[] serializedkinesisClientConfig;
    private final Integer recordsLimit;
    private final Regions region;

    private transient AWSCredentialsProvider credentialsProvider;
    private transient ClientConfiguration clientConfiguration;

    /**
     * Create a new Kinesis connection info.
     * @param credentialsProvider implementation to provide credentials to connect to kinesis
     * @param clientConfiguration client configuration to pass to kinesis client
     * @param region region to connect to
     * @param recordsLimit max records to be fetched in a getRecords request to kinesis
     */
    public KinesisConnectionInfo(AWSCredentialsProvider credentialsProvider,
            ClientConfiguration clientConfiguration,
            Regions region,
            Integer recordsLimit) {
        if (recordsLimit == null || recordsLimit <= 0) {
            throw new IllegalArgumentException("recordsLimit has to be a positive integer");
        }
        if (region == null) {
            throw new IllegalArgumentException("region cannot be null");
        }
        serializedKinesisCredsProvider = getKryoSerializedBytes(credentialsProvider);
        serializedkinesisClientConfig = getKryoSerializedBytes(clientConfiguration);
        this.recordsLimit = recordsLimit;
        this.region = region;
    }

    public Integer getRecordsLimit() {
        return recordsLimit;
    }

    public AWSCredentialsProvider getCredentialsProvider() {
        if (credentialsProvider == null) {
            credentialsProvider = (AWSCredentialsProvider) this.getKryoDeserializedObject(serializedKinesisCredsProvider);
        }
        return credentialsProvider;
    }

    public ClientConfiguration getClientConfiguration() {
        if (clientConfiguration == null) {
            clientConfiguration = (ClientConfiguration) this.getKryoDeserializedObject(serializedkinesisClientConfig);
        }
        return clientConfiguration;
    }

    public Regions getRegion() {
        return region;
    }

    private byte[] getKryoSerializedBytes(final Object obj) {
        final Kryo kryo = new Kryo();
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final Output output = new Output(os);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.writeClassAndObject(output, obj);
        output.flush();
        return os.toByteArray();
    }

    private Object getKryoDeserializedObject(final byte[] ser) {
        final Kryo kryo = new Kryo();
        final Input input = new Input(new ByteArrayInputStream(ser));
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        return kryo.readClassAndObject(input);
    }

    @Override
    public String toString() {
        return "KinesisConnectionInfo{"
                + "serializedKinesisCredsProvider=" + Arrays.toString(serializedKinesisCredsProvider)
                + ", serializedkinesisClientConfig=" + Arrays.toString(serializedkinesisClientConfig)
                + ", region=" + region
                + ", recordsLimit=" + recordsLimit
                + '}';
    }
}
