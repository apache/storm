package backtype.storm.contrib.jms.spout;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.ISpoutOutputCollector;

public class MockSpoutOutputCollector implements ISpoutOutputCollector {
    boolean emitted = false;

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        emitted = true;
        return new ArrayList<Integer>();
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emitted = true;
    }

    public boolean emitted(){
        return this.emitted;
    }
    
    public void reset(){
        this.emitted = false;
    }
}
