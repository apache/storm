package com.alibaba.jstorm.common.metric;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

/**
 * Created by wuchong on 15/10/13.
 */
public class AsmMetricTest {

    @Test
    public void testFlush() throws Exception {
        AsmCounter counter = new AsmCounter();
        counter.setMetricName("mock@metric@name");
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int c = random.nextInt(10);
            counter.update(c);
        }

        counter.flush();

        assertEquals(4, counter.getSnapshots().size());

    }
}