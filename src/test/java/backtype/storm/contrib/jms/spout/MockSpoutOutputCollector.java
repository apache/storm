package backtype.storm.contrib.jms.spout;

import java.util.List;

import backtype.storm.spout.ISpoutOutputCollector;

public class MockSpoutOutputCollector implements ISpoutOutputCollector {

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        throw new RuntimeException("Not implemented yet.");
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        throw new RuntimeException("Not implemented yet.");
    }

}
