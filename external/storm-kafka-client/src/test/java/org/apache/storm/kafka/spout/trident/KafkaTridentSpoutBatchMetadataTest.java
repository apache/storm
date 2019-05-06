/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout.trident;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;
import org.json.simple.JSONValue;
import org.junit.Test;

public class KafkaTridentSpoutBatchMetadataTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void testMetadataIsRoundTripSerializableWithJsonSimple() throws Exception {
        /**
         * Tests that the metadata object can be converted to and from a Map. This is needed because Trident metadata is written to
         * Zookeeper as JSON with the json-simple library, so the spout converts the metadata to Map before returning it to Trident.
         * It is important that all map entries are types json-simple knows about,
         * since otherwise the library just calls toString on them which will likely produce invalid JSON.
         */
        long startOffset = 10;
        long endOffset = 20;
        String topologyId = "topologyId";

        KafkaTridentSpoutBatchMetadata metadata = new KafkaTridentSpoutBatchMetadata(startOffset, endOffset, topologyId);
        Map<String, Object> map = metadata.toMap();
        Map<String, Object> deserializedMap = (Map)JSONValue.parseWithException(JSONValue.toJSONString(map));
        KafkaTridentSpoutBatchMetadata deserializedMetadata = KafkaTridentSpoutBatchMetadata.fromMap(deserializedMap);
        assertThat(deserializedMetadata.getFirstOffset(), is(metadata.getFirstOffset()));
        assertThat(deserializedMetadata.getLastOffset(), is(metadata.getLastOffset()));
        assertThat(deserializedMetadata.getTopologyId(), is(metadata.getTopologyId()));
    }

}
