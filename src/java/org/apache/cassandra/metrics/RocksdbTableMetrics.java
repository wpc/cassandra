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

package org.apache.cassandra.metrics;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.rocksdb.RocksDBConfigs;
import org.apache.cassandra.rocksdb.RocksDBUtils;
import org.apache.cassandra.rocksdb.RocksEngine;
import org.apache.cassandra.rocksdb.encoding.metrics.MetricsFactory;
import org.apache.cassandra.rocksdb.streaming.RocksdbThroughputManager;
import org.rocksdb.HistogramType;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class RocksdbTableMetrics
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksdbTableMetrics.class);
    public final Histogram rocksdbIngestTimeHistogram;
    public final Histogram rocksdbIngestWaitTimeHistogram;

    public final List<Gauge<Integer>> rocksdbNumSstablePerLevel;
    public final Gauge<Long> rocksdbPendingCompactionBytes;

    public final Counter rocksdbIterMove;
    public final Counter rocksdbIterSeek;
    public final Counter rocksdbIterNew;

    static
    {
        Metrics.register(RocksMetricNameFactory.DEFAULT_FACTORY.createMetricName("RocksdbOutgoingThroughput"),
                         new Gauge<Long>()
                         {
                             public Long getValue()
                             {
                                 return RocksdbThroughputManager.getInstance().getOutgoingThroughput();
                             }
                         });

        Metrics.register(RocksMetricNameFactory.DEFAULT_FACTORY.createMetricName("RocksdbIncomingThroughput"),
                         new Gauge<Long>()
                         {
                             public Long getValue()
                             {
                                 return RocksdbThroughputManager.getInstance().getIncomingThroughput();
                             }
                         });
    }

    public RocksdbTableMetrics(ColumnFamilyStore cfs, Statistics stats)
    {
        MetricNameFactory factory = new RocksMetricNameFactory(cfs);

        Metrics.register(factory.createMetricName("WalFileSyncMicros"),
                         MetricsFactory.createHistogram(stats, HistogramType.WAL_FILE_SYNC_MICROS));
        Metrics.register(factory.createMetricName("ManifiestSyncMicros"),
                         MetricsFactory.createHistogram(stats, HistogramType.MANIFEST_FILE_SYNC_MICROS));
        Metrics.register(factory.createMetricName("TableOpenIOMicros"),
                         MetricsFactory.createHistogram(stats, HistogramType.TABLE_OPEN_IO_MICROS));
        Metrics.register(factory.createMetricName("MultiGet"),
                         MetricsFactory.createHistogram(stats, HistogramType.DB_MULTIGET));
        Metrics.register(factory.createMetricName("ReadBlockCompactionMicros"),
                         MetricsFactory.createHistogram(stats, HistogramType.READ_BLOCK_COMPACTION_MICROS));
        Metrics.register(factory.createMetricName("ReadBlockGetMicros"),
                         MetricsFactory.createHistogram(stats, HistogramType.READ_BLOCK_GET_MICROS));
        Metrics.register(factory.createMetricName("WriteRawBlockMicros"),
                         MetricsFactory.createHistogram(stats, HistogramType.WRITE_RAW_BLOCK_MICROS));
        Metrics.register(factory.createMetricName("StallL0SlowdownCount"),
                         MetricsFactory.createHistogram(stats, HistogramType.STALL_L0_SLOWDOWN_COUNT));
        Metrics.register(factory.createMetricName("MemtableCompactionCount"),
                         MetricsFactory.createHistogram(stats, HistogramType.STALL_MEMTABLE_COMPACTION_COUNT));
        Metrics.register(factory.createMetricName("StallL0NumFilesCount"),
                         MetricsFactory.createHistogram(stats, HistogramType.STALL_L0_NUM_FILES_COUNT));
        Metrics.register(factory.createMetricName("HardRateLimitDelayCount"),
                         MetricsFactory.createHistogram(stats, HistogramType.HARD_RATE_LIMIT_DELAY_COUNT));
        Metrics.register(factory.createMetricName("SoftRateLimitDelayCount"),
                         MetricsFactory.createHistogram(stats, HistogramType.SOFT_RATE_LIMIT_DELAY_COUNT));
        Metrics.register(factory.createMetricName("NumFilesInSingleCompaction"),
                         MetricsFactory.createHistogram(stats, HistogramType.NUM_FILES_IN_SINGLE_COMPACTION));
        Metrics.register(factory.createMetricName("DbSeek"),
                         MetricsFactory.createHistogram(stats, HistogramType.DB_SEEK));
        Metrics.register(factory.createMetricName("WriteStall"),
                         MetricsFactory.createHistogram(stats, HistogramType.WRITE_STALL));
        Metrics.register(factory.createMetricName("SstReadMs"),
                         MetricsFactory.createHistogram(stats, HistogramType.SST_READ_MICROS));
        Metrics.register(factory.createMetricName("NumSubCompactionsScheduled"),
                         MetricsFactory.createHistogram(stats, HistogramType.NUM_SUBCOMPACTIONS_SCHEDULED));
        Metrics.register(factory.createMetricName("BytesPerRead"),
                         MetricsFactory.createHistogram(stats, HistogramType.BYTES_PER_READ));
        Metrics.register(factory.createMetricName("BytesPerWrite"),
                         MetricsFactory.createHistogram(stats, HistogramType.BYTES_PER_WRITE));
        Metrics.register(factory.createMetricName("BytesPerMultiget"),
                         MetricsFactory.createHistogram(stats, HistogramType.BYTES_PER_MULTIGET));
        Metrics.register(factory.createMetricName("BytesCompressed"),
                         MetricsFactory.createHistogram(stats, HistogramType.BYTES_COMPRESSED));
        Metrics.register(factory.createMetricName("BytesDecompressed"),
                         MetricsFactory.createHistogram(stats, HistogramType.BYTES_DECOMPRESSED));
        Metrics.register(factory.createMetricName("CompressionTimeUs"),
                         MetricsFactory.createHistogram(stats, HistogramType.COMPRESSION_TIMES_NANOS));
        Metrics.register(factory.createMetricName("DecompressionTimeUs"),
                         MetricsFactory.createHistogram(stats, HistogramType.DECOMPRESSION_TIMES_NANOS));
        Metrics.register(factory.createMetricName("ReadNumMergeOperands"),
                         MetricsFactory.createHistogram(stats, HistogramType.READ_NUM_MERGE_OPERANDS));
        Metrics.register(factory.createMetricName("HistogramEnumMaxHistogram"),
                         MetricsFactory.createHistogram(stats, HistogramType.HISTOGRAM_ENUM_MAX));

        Metrics.register(factory.createMetricName("CompactReadBytes"),
                         MetricsFactory.createCounter(stats, TickerType.COMPACT_READ_BYTES));
        Metrics.register(factory.createMetricName("CompactWriteBytes"),
                         MetricsFactory.createCounter(stats, TickerType.COMPACT_WRITE_BYTES));
        Metrics.register(factory.createMetricName("CompactionKeyDropUser"),
                         MetricsFactory.createCounter(stats, TickerType.COMPACTION_KEY_DROP_USER));
        Metrics.register(factory.createMetricName("NumberKeysWritten"),
                         MetricsFactory.createCounter(stats, TickerType.NUMBER_KEYS_WRITTEN));
        Metrics.register(factory.createMetricName("MemtableHit"),
                         MetricsFactory.createCounter(stats, TickerType.MEMTABLE_HIT));
        Metrics.register(factory.createMetricName("MemtableMiss"),
                         MetricsFactory.createCounter(stats, TickerType.MEMTABLE_MISS));
        Metrics.register(factory.createMetricName("BlockCacheHit"),
                         MetricsFactory.createCounter(stats, TickerType.BLOCK_CACHE_HIT));
        Metrics.register(factory.createMetricName("BlockCacheMiss"),
                         MetricsFactory.createCounter(stats, TickerType.BLOCK_CACHE_MISS));
        Metrics.register(factory.createMetricName("StallMicros"),
                         MetricsFactory.createCounter(stats, TickerType.STALL_MICROS));
        Metrics.register(factory.createMetricName("DBMutexWaitMicros"),
                         MetricsFactory.createCounter(stats, TickerType.DB_MUTEX_WAIT_MICROS));
        Metrics.register(factory.createMetricName("MergeOperationTotalTime"),
                         MetricsFactory.createCounter(stats, TickerType.MERGE_OPERATION_TOTAL_TIME));

        rocksdbIngestTimeHistogram = Metrics.histogram(factory.createMetricName("IngestTime"), true);
        rocksdbIngestWaitTimeHistogram = Metrics.histogram(factory.createMetricName("IngestWaitTime"), true);

        rocksdbNumSstablePerLevel = new ArrayList<>(RocksDBConfigs.MAX_LEVELS);
        for (int level = 0; level < RocksDBConfigs.MAX_LEVELS; level++)
        {
            final int fLevel = level;
            rocksdbNumSstablePerLevel.add(Metrics.register(factory.createMetricName("SSTableCountPerLevel." + fLevel),
                                                           new Gauge<Integer>()
                                                           {
                                                               public Integer getValue()
                                                               {
                                                                   try
                                                                   {
                                                                       return RocksDBUtils.getNumberOfSstablesByLevel(RocksEngine.getRocksDBInstance(cfs), fLevel);
                                                                   }
                                                                   catch (Throwable e)
                                                                   {
                                                                       LOGGER.warn("Failed to get sstable count by level.", e);
                                                                       return 0;
                                                                   }
                                                               }
                                                           }));
        }

        rocksdbPendingCompactionBytes = Metrics.register(factory.createMetricName("PendingCompactionBytes"),
                                                         new Gauge<Long>()
                                                         {
                                                             public Long getValue()
                                                             {
                                                                 try
                                                                 {
                                                                     return RocksDBUtils.getPendingCompactionBytes(RocksEngine.getRocksDBInstance(cfs));
                                                                 }
                                                                 catch (Throwable e)
                                                                 {
                                                                     LOGGER.warn("Failed to get pending compaction bytes", e);
                                                                     return 0L;
                                                                 }
                                                             }
                                                         });

        rocksdbIterMove = Metrics.counter(factory.createMetricName("RocksIterMove"));
        rocksdbIterSeek = Metrics.counter(factory.createMetricName("RocksIterSeek"));
        rocksdbIterNew = Metrics.counter(factory.createMetricName("RocksIterNew"));
    }

    static class RocksMetricNameFactory implements MetricNameFactory
    {
        private static final RocksMetricNameFactory DEFAULT_FACTORY = new RocksMetricNameFactory(null);
        private static final String TYPE = "Rocksdb";
        private final String keyspaceName;
        private final String tableName;

        RocksMetricNameFactory(ColumnFamilyStore cfs)
        {
            if (cfs != null)
            {
                this.keyspaceName = cfs.keyspace.getName();
                this.tableName = cfs.name;
            }
            else
            {
                this.keyspaceName = "all";
                this.tableName = "all";
            }
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = TableMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=" + TYPE);
            mbeanName.append(",keyspace=").append(keyspaceName);
            mbeanName.append(",scope=").append(tableName);
            mbeanName.append(",name=").append(metricName);

            return new CassandraMetricsRegistry.MetricName(groupName, TYPE, metricName, keyspaceName + "." + tableName, mbeanName.toString());
        }
    }
}
