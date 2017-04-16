package org.apache.storm;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.LocalState;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class LocalStateTest {

    @Test
    public void testLocalState() throws Exception{
        try(TmpPath dir1_tmp = new TmpPath();  TmpPath dir2_tmp = new TmpPath() ) {
            GlobalStreamId globalStreamId_a = new GlobalStreamId("a", "a");
            GlobalStreamId globalStreamId_b = new GlobalStreamId("b", "b");
            GlobalStreamId globalStreamId_c = new GlobalStreamId("c", "c");
            GlobalStreamId globalStreamId_d = new GlobalStreamId("d", "d");

            LocalState ls1 = new LocalState(dir1_tmp.getPath());
            LocalState ls2 = new LocalState(dir2_tmp.getPath());
            Assert.assertTrue(ls1.snapshot().isEmpty());

            ls1.put("a", globalStreamId_a);
            ls1.put("b", globalStreamId_b);
            Assert.assertTrue(ls1.snapshot().containsKey("a"));
            Assert.assertTrue(ls1.snapshot().containsValue(globalStreamId_a));
            Assert.assertTrue(ls1.snapshot().containsKey("b"));
            Assert.assertTrue(ls1.snapshot().containsValue(globalStreamId_b));
            Assert.assertEquals(globalStreamId_a, ls1.get("a"));
            Assert.assertEquals(null, ls1.get("c"));
            Assert.assertEquals(globalStreamId_b, ls1.get("b"));
            Assert.assertTrue(ls2.snapshot().isEmpty());

            ls2.put("b", globalStreamId_a);
            ls2.put("b", globalStreamId_b);
            ls2.put("b", globalStreamId_c);
            ls2.put("b", globalStreamId_d);
            Assert.assertEquals(globalStreamId_d, ls2.get("b"));
        }
    }
}
