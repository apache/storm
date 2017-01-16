package org.apache.storm.kafka.spout;

import org.apache.storm.tuple.Fields;
import org.junit.Assert;
import org.junit.Test;

public class KafkaSpoutStreamsNamedTopicsTest {

	@Test
	public void testGetOutputFields() {
		Fields outputFields = new Fields("b","a");
		String[] topics = new String[]{"testTopic"};
		String streamId = "test";
		KafkaSpoutStreamsNamedTopics build = new KafkaSpoutStreamsNamedTopics.Builder(outputFields, streamId, topics)
        .addStream(outputFields, streamId, topics)  // contents of topic test2 sent to test2_stream
        .build();
		Fields actualFields = build.getOutputFields();
		Assert.assertEquals(outputFields.get(0), actualFields.get(0));
		Assert.assertEquals(outputFields.get(1), actualFields.get(1));

	}

}
