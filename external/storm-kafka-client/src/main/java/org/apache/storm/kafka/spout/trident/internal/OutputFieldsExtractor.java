/*
 * Copyright 2018 The Apache Software Foundation.
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

package org.apache.storm.kafka.spout.trident.internal;

import java.io.Serializable;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutConfig;
import org.apache.storm.tuple.Fields;

public class OutputFieldsExtractor implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Extract the output fields from the config.
     * Throws an error if there are multiple declared output streams, since Trident only supports one output stream per spout.
     */
    public <K, V> Fields getOutputFields(KafkaTridentSpoutConfig<K, V> kafkaSpoutConfig) {
        RecordTranslator<K, V> translator = kafkaSpoutConfig.getTranslator();
        int numStreams = translator.streams().size();
        if (numStreams > 1) {
            throw new IllegalStateException("Trident spouts must have at most one output stream,"
                + " found streams [" + translator.streams() + "]");
        }
        return translator.getFieldsFor(translator.streams().get(0));
    }
    
}
