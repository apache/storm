package org.apache.storm;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.LocalState;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class LocalStateTest {

    @Test
    public void testLocalState() throws IOException{
        TmpPath dir1_tmp = new TmpPath();
        TmpPath dir2_tmp = new TmpPath();
        GlobalStreamId globalStreamId_a = new GlobalStreamId("a","a");
        GlobalStreamId globalStreamId_b = new GlobalStreamId("b","b");
        GlobalStreamId globalStreamId_c = new GlobalStreamId("c","c");
        GlobalStreamId globalStreamId_d = new GlobalStreamId("d","d");

        LocalState ls1 = new LocalState(dir1_tmp.getPath());
        LocalState ls2 = new LocalState(dir2_tmp.getPath());
        Assert.assertTrue(ls1.snapshot().isEmpty());

        ls1.put("a",globalStreamId_a);
        ls1.put("b",globalStreamId_b);
        Assert.assertTrue(ls1.snapshot().containsKey("a"));
        Assert.assertTrue(ls1.snapshot().containsValue(globalStreamId_a));
        Assert.assertTrue(ls1.snapshot().containsKey("b"));
        Assert.assertTrue(ls1.snapshot().containsValue(globalStreamId_b));

        ls2.put("b",globalStreamId_a);
        ls2.put("b",globalStreamId_b);
        ls2.put("b",globalStreamId_c);
        ls2.put("b",globalStreamId_d);
        Assert.assertEquals(globalStreamId_d, ls2.get("b"));
    }

    @Test
    public void testEmptyState() throws IOException {

        TmpPath tmp_dir = new TmpPath();
        String dir = tmp_dir.getPath();
        LocalState ls = new LocalState(dir);
        GlobalStreamId gs_a = new GlobalStreamId("a","a");
        //FileOutputStream data =FileUtils.openOutputStream(new File(dir,"12345"));
        //FileOutputStream version =FileUtils.openOutputStream(new File(dir,"12345.version"));
        Assert.assertNull(ls.get("c"));
        ls.put("a",gs_a);
        Assert.assertEquals(gs_a, ls.get("a"));

    }
}
