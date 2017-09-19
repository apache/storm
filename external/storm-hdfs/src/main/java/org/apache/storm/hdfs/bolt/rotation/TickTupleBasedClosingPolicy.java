package org.apache.storm.hdfs.bolt.rotation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of closing files policy waits for tick tuple and at each tick tuple increments its counter.
 *  counter > threshold will indicate if file rotation is needed.
 */
public class TickTupleBasedClosingPolicy implements ClosingFilesPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(TickTupleBasedClosingPolicy.class);
    private final int threshold;
    private int count;

    public TickTupleBasedClosingPolicy(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean closeWriter() {
        LOG.debug( "Threshold: " + threshold+ "count: " + count );
        this.count++;

        if(this.count > this.threshold) {
            LOG.info("Count exceeded threshold: ");
            reset();
            return true;
        }
        return false;

    }
    // This is invoked for each tuple consumed for current writer.
    @Override
    public void reset() {
        this.count =0;
    }

    @Override
    public ClosingFilesPolicy copy() {
        return new TickTupleBasedClosingPolicy(this.threshold);
    }
}
