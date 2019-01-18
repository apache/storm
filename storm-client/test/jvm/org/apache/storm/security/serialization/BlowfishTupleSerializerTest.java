/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.shade.com.google.common.base.Joiner;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.utils.ListDelegate;
import org.junit.Assert;
import org.junit.Test;

public class BlowfishTupleSerializerTest {

    /**
     * Throws RuntimeException when no encryption key is given.
     */
    @Test(expected = RuntimeException.class)
    public void testConstructorThrowsOnNullKey() {
        new BlowfishTupleSerializer(null, new HashMap());
    }

    /**
     * Throws RuntimeException when an invalid encryption key is given.
     */
    @Test(expected = RuntimeException.class)
    public void testConstructorThrowsOnInvalidKey() {
        // The encryption key must be hexadecimal.
        new BlowfishTupleSerializer(null, ImmutableMap.of(BlowfishTupleSerializer.SECRET_KEY, "0123456789abcdefg"));
    }

    /**
     * Test using {@link org.apache.storm.security.serialization.BlowfishTupleSerializer#SECRET_KEY}.
     */
    @Test
    public void testUseBlowfishKey() {
        String arbitraryKey = "7dd6fb3203878381b08f9c89d25ed105";
        Map<String, Object> topoConf = ImmutableMap.of(BlowfishTupleSerializer.SECRET_KEY, arbitraryKey);
        testEncryptsAndDecryptsMessage(topoConf);
    }

    /**
     * Test using {@link org.apache.storm.Config#STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD}
     * when {@link org.apache.storm.security.serialization.BlowfishTupleSerializer#SECRET_KEY} is not present.
     */
    @Test
    public void testUseZookeeperSecret() {
        Map<String, Object> topoConf = ImmutableMap.of(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "user:password");
        testEncryptsAndDecryptsMessage(topoConf);
    }

    /**
     * Reads a string encrypted by another instance with a shared key
     */
    private void testEncryptsAndDecryptsMessage(Map<String, Object> topoConf) {
        String testText = "Tetraodontidae is a family of primarily marine and estuarine fish of the order" +
                          " Tetraodontiformes. The family includes many familiar species, which are" +
                          " variously called pufferfish, puffers, balloonfish, blowfish, bubblefish," +
                          " globefish, swellfish, toadfish, toadies, honey toads, sugar toads, and sea" +
                          " squab.[1] They are morphologically similar to the closely related" +
                          " porcupinefish, which have large external spines (unlike the thinner, hidden" +
                          " spines of Tetraodontidae, which are only visible when the fish has puffed up)." +
                          " The scientific name refers to the four large teeth, fused into an upper and" +
                          " lower plate, which are used for crushing the shells of crustaceans and" +
                          " mollusks, their natural prey.";
        Kryo kryo = new Kryo();
        BlowfishTupleSerializer writerBTS = new BlowfishTupleSerializer(kryo, topoConf);
        BlowfishTupleSerializer readerBTS = new BlowfishTupleSerializer(kryo, topoConf);
        int bufferSize = 1024;
        Output output = new Output(bufferSize, bufferSize);
        Input input = new Input(bufferSize);
        String[] stringList = testText.split(" ");
        ListDelegate delegate = new ListDelegate();
        delegate.addAll(Arrays.asList(stringList));

        writerBTS.write(kryo, output, delegate);
        input.setBuffer(output.getBuffer());
        ListDelegate outDelegate = readerBTS.read(kryo, input, ListDelegate.class);
        Assert.assertEquals(testText, Joiner.on(" ").join(outDelegate.toArray()));
    }
}
