package com.alibaba.jstorm.common.metric.old.window;

import junit.framework.TestCase;

/**
 * Created by wuchong on 15/8/11.
 */
public class StatBucketsTest extends TestCase {

    public void testPrettyUptime() throws Exception {
        int secs = 10860;
        assertEquals("3h 1m 0s", StatBuckets.prettyUptime(secs));

        secs = 203010;
        assertEquals("2d 8h 23m 30s", StatBuckets.prettyUptime(secs));

        secs = 234;
        assertEquals("3m 54s", StatBuckets.prettyUptime(secs));

        secs = 32;
        assertEquals("32s", StatBuckets.prettyUptime(secs));

        secs = 0;
        assertEquals("0s", StatBuckets.prettyUptime(secs));

    }
}