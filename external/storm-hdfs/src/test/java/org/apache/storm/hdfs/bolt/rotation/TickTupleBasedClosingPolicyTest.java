package org.apache.storm.hdfs.bolt.rotation;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;


public class TickTupleBasedClosingPolicyTest {

    ClosingFilesPolicy filePolicy;

    @Before
    public void setup() {
        filePolicy = new TickTupleBasedClosingPolicy(1);
    }

    @Test
    public void closeWriter_belowThreshold()
    {
        assertFalse(filePolicy.closeWriter());
    }
    @Test
    public void closeWriter_aboveThreshold()
    {
        assertFalse(filePolicy.closeWriter());
        assertTrue(filePolicy.closeWriter());
    }
}
