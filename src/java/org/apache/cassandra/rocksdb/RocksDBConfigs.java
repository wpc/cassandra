package org.apache.cassandra.rocksdb;

import java.io.File;

public class RocksDBConfigs
{
    // Paths for storing RocksDB files.
    public static String ROCKSDB_DIR = System.getProperty("cassandra.rocksdb.dir", "/data/rocksdb");
    public static File STREAMING_TMPFILE_PATH = new File(System.getProperty("cassandra.rocksdb.stream.dir", "/data/rocksdbstream/"));

    // Max levels for RocksDB. 7 is the default value.
    public static int MAX_LEVELS = 7;

    // Tables created in this keyspace is backed by RocksDB.
    public static String ROCKSDB_KEYSPACE = System.getProperty("cassandra.rocksdb.keyspace", "rocksdb");

    // The read buffer size while reading RocksDB and sending into output stream.
    public static long STREAMING_READ_AHEAD_BUFFER_SIZE = Long.getLong("cassandra.rocksdb.stream.readahead_size", 10L * 1024 * 1024);

    // RocksDB stopped write while number of level 0 sstables exceed this threshold. By default we set it to
    // a very large number to trade read performance for high stream throughput.
    public static int LEVEL0_STOP_WRITES_TRIGGER = Integer.getInteger("cassandra.rocksdb.level0_stop_writes_trigger", 1024);

    // On the receiver side, we create a sstable and feed it to RocksDB every 512MB received.
    public static long SSTABLE_INGEST_THRESHOLD = Long.parseLong(System.getProperty("cassandra.rocksdb.stream.sst_size", "536870912"));

    // Once enabled, the writes are written to both Cassandra and RocksDB which is future used to caclulate
    // the consistency and correctness of RocksDB.
    public static boolean ROCKSDB_DOUBLE_WRITE = Boolean.getBoolean("cassandra.rocksdb.double_write");
}
