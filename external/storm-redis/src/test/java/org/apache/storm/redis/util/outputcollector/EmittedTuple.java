package org.apache.storm.redis.util.outputcollector;

import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Used with StubOutputCollector for testing.
 */
public class EmittedTuple {
    private final String streamId;
    private final List<Object> tuple;
    private final List<Tuple> anchors;

    public EmittedTuple(final String streamId, final List<Object> tuple, final Collection<Tuple> anchors) {
        this.streamId = streamId;
        this.tuple = tuple;
        this.anchors = new ArrayList<>(anchors);
    }

    public String getStreamId() {
        return streamId;
    }

    public List<Object> getTuple() {
        return tuple;
    }

    public List<Tuple> getAnchors() {
        return Collections.unmodifiableList(anchors);
    }

    @Override
    public String toString() {
        return "EmittedTuple{"
            + "streamId='" + streamId + '\''
            + ", tuple=" + tuple
            + ", anchors=" + anchors
            + '}';
    }
}
