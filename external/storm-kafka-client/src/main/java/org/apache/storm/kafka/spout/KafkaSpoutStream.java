/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Represents the stream and output fields used by a topic
 */
public class KafkaSpoutStream implements Serializable {

		private final Fields outputFields;
    private final String streamId;
    private final String topic;
    private final Pattern topicWildcard;

    /** Represents the specified outputFields and topic with the default stream */
    KafkaSpoutStream(Fields outputFields, String topic) {
        this(outputFields, Utils.DEFAULT_STREAM_ID, topic);
    }

    /** Represents the specified outputFields and topic with the specified stream */
    KafkaSpoutStream(Fields outputFields, String streamId, String topic) {
        if (outputFields == null || streamId == null || topic == null) {
            throw new IllegalArgumentException(String.format("Constructor parameters cannot be null. " +
                    "[outputFields=%s, streamId=%s, topic=%s]", outputFields, streamId, topic));
        }
        this.outputFields = outputFields;
        this.streamId = streamId;
        this.topic = topic;
        this.topicWildcard = null;
    }
    
    public KafkaSpoutStream(Fields outputFields, String streamId, Pattern topicWildcard) {
      if (outputFields == null || streamId == null || topicWildcard == null) {
          throw new IllegalArgumentException(String.format("Constructor parameters cannot be null. " +
                  "[outputFields=%s, streamId=%s, topicwildcard=%s]", outputFields, streamId, topicWildcard.pattern()));
      }
      this.outputFields = outputFields;
      this.streamId = streamId;
      this.topic = null;
      this.topicWildcard = topicWildcard;
      
    }

    Fields getOutputFields() {
        return outputFields;
    }

    String getStreamId() {
        return streamId;
    }

    String getTopic() {
        return topic;
    }
    
    Pattern getTopicWildcard() {
			return topicWildcard;
		}

		public void emit(SpoutOutputCollector collector, List<Object> tuple, KafkaSpoutMessageId messageId) {
      collector.emit(getStreamId(), tuple, messageId);
    }
    
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(getStreamId(), getOutputFields());
    }

    @Override
    public String toString() {
        return "KafkaSpoutStream{" +
                "outputFields=" + outputFields +
                ", streamId='" + streamId + '\'' +
                ", topic='" + topic == null ? "" : topic + '\'' +
                ", topicWildcard='" + topicWildcard == null ? "" : topicWildcard.pattern() + '\'' +
                '}';
    }
}
