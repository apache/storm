package org.apache.storm.metricstore.rocksdb;

import org.apache.storm.metricstore.MetricException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDB;

public class StringMetadataCacheTest {

    @Before
    public void setUp() {
        // remove any previously created cache instance
        StringMetadataCache.cleanUp();
        RocksDB.loadLibrary();
    }

    private class TestDbWriter extends RocksDbMetricsWriter {
        boolean evictCalled = false;

        TestDbWriter() {
            super(null, null, null);
        }

        @Override
        void handleEvictedMetadata(RocksDbKey key, RocksDbValue val) {
            evictCalled = true;
        }
    }

    @After
    public void tearDown() {
        StringMetadataCache.cleanUp();
    }

    @Test
    public void validateEviction() throws MetricException {
        TestDbWriter writer = new TestDbWriter();
        StringMetadataCache.init(writer, 2);
        WritableStringMetadataCache wCache = StringMetadataCache.getWritableStringMetadataCache();
        ReadOnlyStringMetadataCache rCache = StringMetadataCache.getReadOnlyStringMetadataCache();

        String s1 = "string1";
        Integer s1Id = 1;
        long s1Timestamp = 1L;
        StringMetadata metadata1 = new StringMetadata(KeyType.STREAM_ID_STRING, s1Id, s1Timestamp);
        wCache.put(s1, metadata1);
        Assert.assertEquals(metadata1, rCache.get(s1));
        Assert.assertTrue(rCache.contains(s1Id));
        Assert.assertEquals(s1, rCache.getMetadataString(s1Id));

        String s2 = "string2";
        Integer s2Id = 2;
        long s2Timestamp = 2L;
        StringMetadata metadata2 = new StringMetadata(KeyType.EXEC_ID_STRING, s2Id, s2Timestamp);
        wCache.put(s2, metadata2);
        Assert.assertEquals(metadata2, rCache.get(s2));
        Assert.assertTrue(rCache.contains(s2Id));
        Assert.assertEquals(s2, rCache.getMetadataString(s2Id));

        Assert.assertEquals(false, writer.evictCalled);

        // read s1 last....  This should cause s2 to be evicted on next put
        rCache.get(s1);

        String s3 = "string3";
        Integer s3Id = 3;
        long s3Timestamp = 3L;
        StringMetadata metadata3 = new StringMetadata(KeyType.TOPOLOGY_STRING, s3Id, s3Timestamp);
        wCache.put(s3, metadata3);

        Assert.assertEquals(true, writer.evictCalled);
        Assert.assertEquals(metadata3, rCache.get(s3));
        Assert.assertTrue(rCache.contains(s3Id));
        Assert.assertEquals(s3, rCache.getMetadataString(s3Id));

        // since s2 read last, it should be evicted, s1 and s3 should exist
        Assert.assertEquals(null, rCache.get(s2));
        Assert.assertFalse(rCache.contains(s2Id));
        Assert.assertEquals(metadata1, rCache.get(s1));
        Assert.assertTrue(rCache.contains(s1Id));
        Assert.assertEquals(s1, rCache.getMetadataString(s1Id));

        StringMetadataCache.cleanUp();
    }

    @Test
    public void validateMultipleKeyTypes() throws MetricException {
        TestDbWriter writer = new TestDbWriter();
        StringMetadataCache.init(writer, 2);
        WritableStringMetadataCache wCache = StringMetadataCache.getWritableStringMetadataCache();

        StringMetadata metadata = new StringMetadata(KeyType.STREAM_ID_STRING, 1, 1L);
        wCache.put("default", metadata);

        metadata = wCache.get("default");
        metadata.update(3L, KeyType.COMPONENT_STRING);

        metadata = wCache.get("default");
        metadata.update(2L, KeyType.STREAM_ID_STRING);

        metadata = wCache.get("default");
        Assert.assertEquals(2, metadata.getMetadataTypes().size());
        Assert.assertTrue(metadata.getMetadataTypes().contains(KeyType.STREAM_ID_STRING));
        Assert.assertTrue(metadata.getMetadataTypes().contains(KeyType.COMPONENT_STRING));
        Assert.assertEquals(3L, metadata.getLastTimestamp());

        StringMetadataCache.cleanUp();
    }
}
