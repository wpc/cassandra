package org.apache.cassandra.rocksdb;

import java.net.URLClassLoader;
import org.junit.Test;
import org.rocksdb.CompressionType;


import static junit.framework.Assert.assertEquals;


public class RocksDBConfigTest
{
    @Test
    public void CompressionTypeDefault()
    {
        RocksDBConfigs configs = new RocksDBConfigs();
        assertEquals(configs.COMPRESSION_TYPE.getValue(), CompressionType.LZ4_COMPRESSION.getValue());
        assertEquals(configs.BOTTOMMOST_COMPRESSION.getValue(), CompressionType.LZ4_COMPRESSION.getValue());
    }

    @Test
    public void testCompressionTypeWithProperty()
    {
        ClassLoader testClassLoader = new TestClassLoader();

        try {
            Class<?> configClass = Class.forName(RocksDBConfigs.class.getName(),
                                                  true, testClassLoader);
            RocksDBConfigs configs = (RocksDBConfigs) configClass.newInstance();
            // After setting the bottommost_compression and compression_type through System properties
            System.setProperty("cassandra.rocksdb.bottommost_compression", "zstd");
            System.setProperty("cassandra.rocksdb.compression_type", "zstd");
            assertEquals(configs.BOTTOMMOST_COMPRESSION.getValue(), CompressionType.ZSTD_COMPRESSION.getValue());
            assertEquals(configs.COMPRESSION_TYPE.getValue(), CompressionType.ZSTD_COMPRESSION.getValue());


        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    // custom class loader to reload classes
    private class TestClassLoader extends URLClassLoader
    {
        public TestClassLoader() {
            super(((URLClassLoader) getSystemClassLoader()).getURLs());
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (name.startsWith("org.apache.cassandra.rocksdb;")) {
                return super.findClass(name);
            }

            return super.loadClass(name);
        }
    }

}
