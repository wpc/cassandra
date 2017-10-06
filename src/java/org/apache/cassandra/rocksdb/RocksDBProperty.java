package org.apache.cassandra.rocksdb;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBProperty
{
    public static int getNumberOfSstablesByLevel(RocksDBCF rocksDBCF, int level) throws RocksDBException
    {
        return Integer.parseInt(rocksDBCF.getProperty("rocksdb.num-files-at-level" + level));
    }

    public static long getPendingCompactionBytes(RocksDBCF rocksDBCF) throws RocksDBException
    {
        return Long.parseLong(rocksDBCF.getProperty("rocksdb.estimate-pending-compaction-bytes"));
    }

    public static long getEstimatedLiveDataSize(RocksDBCF rocksDBCF) throws RocksDBException
    {
        return Long.parseLong(rocksDBCF.getProperty("rocksdb.estimate-live-data-size"));
    }

    public static long getEstimatedNumKeys(RocksDBCF rocksDBCF) throws RocksDBException
    {
        return Long.parseLong(rocksDBCF.getProperty("rocksdb.estimate-num-keys"));
    }
}
