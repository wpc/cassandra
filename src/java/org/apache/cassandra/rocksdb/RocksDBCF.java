/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.rocksdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.RocksDBTableMetrics;
import org.apache.cassandra.rocksdb.encoding.RowKeyEncoder;
import org.apache.cassandra.rocksdb.encoding.orderly.Bytes;
import org.apache.cassandra.utils.Hex;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CassandraCompactionFilter;
import org.rocksdb.CassandraValueMergeOperator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.WriteOptions;

import static org.apache.cassandra.rocksdb.RocksDBConfigs.ROCKSDB_DIR;

/**
 * A wrapper around RocksDB instance.
 */
public class RocksDBCF
{
    private static final Logger logger = LoggerFactory.getLogger(RocksDBCF.class);
    private final UUID cfID;
    private final ColumnFamilyStore cfs;
    private final IPartitioner partitioner;
    private final RocksDBEngine engine;
    private final RocksDB rocksDB;
    private final Statistics stats;
    private final RocksDBTableMetrics rocksMetrics;
    private final CassandraCompactionFilter compactionFilter;

    private final ReadOptions readOptions;
    private final WriteOptions disableWAL;
    private final FlushOptions flushOptions;
    private final CassandraValueMergeOperator mergeOperator;

    public RocksDBCF(ColumnFamilyStore cfs) throws RocksDBException
    {
        this.cfs = cfs;
        cfID = cfs.metadata.cfId;
        partitioner = cfs.getPartitioner();
        engine = (RocksDBEngine) cfs.engine;

        final long writeBufferSize = 8 * 512 * 1024 * 1024L;
        final long softPendingCompactionBytesLimit = 100 * 64 * 1073741824L;
        DBOptions dbOptions = new DBOptions();
        stats = new Statistics();
        stats.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        compactionFilter = new CassandraCompactionFilter(cfs.metadata.params.purgeTtlOnExpiration);
        mergeOperator = new CassandraValueMergeOperator(cfs.metadata.params.gcGraceSeconds);

        dbOptions.setCreateIfMissing(true);
        dbOptions.setAllowConcurrentMemtableWrite(true);
        dbOptions.setEnableWriteThreadAdaptiveYield(true);
        dbOptions.setBytesPerSync(1024 * 1024);
        dbOptions.setWalBytesPerSync(1024 * 1024);
        dbOptions.setMaxBackgroundCompactions(RocksDBConfigs.BACKGROUD_COMPACTIONS);
        dbOptions.setBaseBackgroundCompactions(RocksDBConfigs.BACKGROUD_COMPACTIONS);
        dbOptions.setMaxSubcompactions(8);
        dbOptions.setStatistics(stats);
        dbOptions.setRateLimiter(engine.rateLimiter);

        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        columnFamilyOptions.setNumLevels(RocksDBConfigs.MAX_LEVELS);
        columnFamilyOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        columnFamilyOptions.setWriteBufferSize(writeBufferSize);
        columnFamilyOptions.setMaxBytesForLevelBase(4 * writeBufferSize);
        columnFamilyOptions.setSoftPendingCompactionBytesLimit(softPendingCompactionBytesLimit);
        columnFamilyOptions.setHardPendingCompactionBytesLimit(8 * softPendingCompactionBytesLimit);
        columnFamilyOptions.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
        columnFamilyOptions.setMergeOperator(mergeOperator);
        columnFamilyOptions.setCompactionFilter(compactionFilter);
        columnFamilyOptions.setLevel0SlowdownWritesTrigger(RocksDBConfigs.LEVEL0_STOP_WRITES_TRIGGER);
        columnFamilyOptions.setLevel0StopWritesTrigger(RocksDBConfigs.LEVEL0_STOP_WRITES_TRIGGER);

        final org.rocksdb.BloomFilter bloomFilter = new BloomFilter(10, false);
        final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setFilter(bloomFilter);
        columnFamilyOptions.setTableFormatConfig(tableOptions);

        ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions);

        String rocksDBTableDir = ROCKSDB_DIR + "/" + cfs.keyspace.getName() + "/" + cfs.name;
        FileUtils.createDirectory(ROCKSDB_DIR);
        FileUtils.createDirectory(rocksDBTableDir);
        rocksDB = RocksDB.open(dbOptions, rocksDBTableDir, Collections.singletonList(columnFamilyDescriptor), new ArrayList<>(1));
        logger.info("Open rocksdb instance for cf {}.{} with path:{}, purgeTTL:{}",
                    cfs.keyspace.getName(), cfs.name, rocksDBTableDir, cfs.metadata.params.purgeTtlOnExpiration);

        rocksMetrics = new RocksDBTableMetrics(cfs, stats);

        // Set `ignore_range_deletion` to speed up read, with the cost of read the stale(range deleted) keys
        // until compaction happens. However in our case, range deletion is only used to remove ranges
        // no longer owned by this node. In such case, stale keys would never be quried.
        readOptions = new ReadOptions().setIgnoreRangeDeletions(true);
        disableWAL = new WriteOptions().setDisableWAL(true);
        flushOptions = new FlushOptions().setWaitForFlush(true);
    }

    public RocksDB getRocksDB()
    {
        return rocksDB;
    }

    public Statistics getStatistics()
    {
        return stats;
    }

    public UUID getCfID()
    {
        return cfID;
    }

    public RocksDBTableMetrics getRocksMetrics()
    {
        return rocksMetrics;
    }

    public byte[] get(byte[] key) throws RocksDBException
    {
        return rocksDB.get(readOptions, key);
    }

    public RocksDBIteratorAdapter newIterator()
    {
        return newIterator(readOptions);
    }

    public RocksDBIteratorAdapter newIterator(ReadOptions options)
    {
        rocksMetrics.rocksDBIterNew.inc();
        return new RocksDBIteratorAdapter(rocksDB.newIterator(options), rocksMetrics);
    }

    public void merge(byte[] key, byte[] value) throws RocksDBException
    {
        merge(key, value, true);
    }

    public void merge(byte[] key, byte[] value, boolean writeCommitLog) throws RocksDBException
    {
        if (writeCommitLog)
        {
            rocksDB.merge(key, value);
        }
        else
        {
            rocksDB.merge(disableWAL, key, value);
        }
    }

    public void deleteRange(byte[] start, byte[] end) throws RocksDBException
    {
        rocksDB.deleteRange(start, end);
    }

    public void compactRange() throws RocksDBException
    {
        rocksDB.compactRange();
    }

    public void forceFlush() throws RocksDBException
    {
        logger.info("Flushing rocksdb table: " + cfs.name);
        rocksDB.flush(flushOptions);
    }

    public String getProperty(String property) throws RocksDBException
    {
        return rocksDB.getProperty(property);
    }

    public void truncate() throws RocksDBException
    {
        // TODO: use delete range for now, could have a better solution
        rocksDB.deleteRange(RowKeyEncoder.encodeToken(RocksDBUtils.getMinToken(partitioner)),
                            RowKeyEncoder.encodeToken(RocksDBUtils.getMaxToken(partitioner)));
    }

    protected void close() throws RocksDBException
    {
        logger.info("Closing rocksdb table: " + cfs.name);
        synchronized (engine.rocksDBFamily)
        {
            rocksDB.close();

            // remove the rocksdb instance, since it's not usable
            engine.rocksDBFamily.remove(cfID);
        }
    }

    public String dumpPrefix(byte[] rocksKeyPrefix, int limit)
    {
        StringBuilder sb = new StringBuilder();
        try (RocksDBIteratorAdapter rocksIterator = newIterator())
        {
            rocksIterator.seek(rocksKeyPrefix);
            while (limit > 0 && rocksIterator.isValid())
            {
                byte[] key = rocksIterator.key();
                if (!Bytes.startsWith(key, rocksKeyPrefix))
                {
                    break;
                }
                sb.append("0x")
                  .append(Hex.bytesToHex(key))
                  .append('\t');

                byte[] value = rocksIterator.value();
                if (value == null)
                {
                    sb.append("null\n");
                }
                else
                {
                    sb.append("0x")
                      .append(Hex.bytesToHex(value))
                      .append('\n');
                }
                limit--;
                rocksIterator.next();
            }
        }
        return sb.toString();
    }
}
