package org.apache.cassandra.rocksdb;

import org.junit.Test;

import org.apache.cassandra.rocksdb.RocksDBConfigs;
import org.rocksdb.CompressionType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RocksDBConfigTest
{
    @Test
    public void testBottommostCompressionTypeDefault()
    {
        // After setting the bottommost_compression for rocksdb through System properties
        System.setProperty("cassandra.rocksdb.bottommost_compression", "zstd");
        RocksDBConfigs configs = new RocksDBConfigs();
        assertEquals(configs.BOTTOMMOST_COMPRESSION.getValue(), CompressionType.ZSTD_COMPRESSION.getValue());
    }
}
