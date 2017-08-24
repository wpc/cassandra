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
import org.apache.cassandra.rocksdb.encoding.metrics.HistogramUtils;
import org.apache.cassandra.rocksdb.streaming.RocksDBSStableWriter;
import org.apache.cassandra.rocksdb.streaming.RocksdbThroughputManager;
import org.rocksdb.HistogramType;
import org.rocksdb.Statistics;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class RocksdbTableMetrics
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksdbTableMetrics.class);
    private final MetricNameFactory factory;
    public final Histogram rocksdbGetHistogram;
    public final Histogram rocksdbWritedHistogram;
    public final Histogram rocksdbTableSyncHistogram;
    public final Histogram rocksdbCompactionOutFileSyncHistogram;
    public final Histogram rocksdbWalFileSyncHistogram;
    public final Histogram rocksdbManifiestSyncHistogram;
    public final Histogram rocksdbTableOpenIoHistogram;
    public final Histogram rocksdbMultiGetHistogram;
    public final Histogram rocksdbReadBlockCompactionHistogram;
    public final Histogram rocksdbReadBlockGetHistogram;
    public final Histogram rocksdbWriteRawBlockHistogram;
    public final Histogram rocksdbStallL0SlowdownHistogram;
    public final Histogram rocksdbMemtableCompactionHistogram;
    public final Histogram rocksdbStallL0NumFilesHistogram;
    public final Histogram rocksdbHardRateLimitDelayHistogram;
    public final Histogram rocksdbSoftRateLimitDelayHistogram;
    public final Histogram rocksdbNumFilesInSingleCompactionHistogram;
    public final Histogram rocksdbDbSeekHistogram;
    public final Histogram rocksdbWriteStallHistogram;
    public final Histogram rocksdbSstReadMsHistogram;
    public final Histogram rocksdbNumSubcompactionsScheduledHistogram;
    public final Histogram rocksdbBytesPerReadHistogram;
    public final Histogram rocksdbBytesPerWriteHistogram;
    public final Histogram rocksdbBytesPerMultiGetHistogram;
    public final Histogram rocksdbBytesCompressedHistogram;
    public final Histogram rocksdbBytesDecompressedHistogram;
    public final Histogram rocksdbCompressionTimeUsHistogram;
    public final Histogram rocksdbDecompressionTimeUsHistogram;
    public final Histogram rocksdbReadNumMergeOperandsHistogram;
    public final Histogram rocksdbHistogramEnumMaxHistogram;
    public final Histogram rocksdbIngestTimeHistogram;
    public final Histogram rocksdbIngestWaitTimeHistogram;

    public static final Gauge<Long> rocksdbOutgoingThroughput;
    public static final Gauge<Long> rocksdbIncomingThroughput;

    public final List<Gauge<Integer>> rocksdbNumSstablePerLevel;
    public final Gauge<Long> rocksdbPendingCompactionBytes;

    public final Counter rocksdbIterMove;
    public final Counter rocksdbIterSeek;
    public final Counter rocksdbIterNew;

    static
    {
        rocksdbOutgoingThroughput = Metrics.register(RocksMetricNameFactory.DEFAULT_FACTORY.createMetricName("RocksdbOutgoingThroughput"),
                                                    new Gauge<Long>(){
                                                        public Long getValue()
                                                        {
                                                            return RocksdbThroughputManager.getInstance().getOutgoingThroughput();
                                                        }
                                                    });

        rocksdbIncomingThroughput = Metrics.register(RocksMetricNameFactory.DEFAULT_FACTORY.createMetricName("RocksdbIncomingThroughput"),
                                                    new Gauge<Long>(){
                                                        public Long getValue()
                                                        {
                                                            return RocksdbThroughputManager.getInstance().getIncomingThroughput();
                                                        }
                                                    });
    }

    public RocksdbTableMetrics(ColumnFamilyStore cfs, Statistics stats) {
        factory = new RocksMetricNameFactory(cfs);

        rocksdbGetHistogram = Metrics.register(factory.createMetricName("Get"),
                                               HistogramUtils.createHistogram(cfs, stats, HistogramType.DB_GET));
        rocksdbWritedHistogram = Metrics.register(factory.createMetricName("Write"),
                                                  HistogramUtils.createHistogram(cfs, stats, HistogramType.DB_WRITE));
        rocksdbTableSyncHistogram = Metrics.register(factory.createMetricName("TableSyncMicros"),
                                                     HistogramUtils.createHistogram(cfs, stats, HistogramType.TABLE_SYNC_MICROS));
        rocksdbCompactionOutFileSyncHistogram = Metrics.register(factory.createMetricName("CompactionOutFileSyncMicros"),
                                                                 HistogramUtils.createHistogram(cfs, stats, HistogramType.COMPACTION_OUTFILE_SYNC_MICROS));
        rocksdbWalFileSyncHistogram = Metrics.register(factory.createMetricName("WalFileSyncMicros"),
                                                       HistogramUtils.createHistogram(cfs, stats, HistogramType.WAL_FILE_SYNC_MICROS));
        rocksdbManifiestSyncHistogram = Metrics.register(factory.createMetricName("ManifiestSyncMicros"),
                                                         HistogramUtils.createHistogram(cfs, stats, HistogramType.MANIFEST_FILE_SYNC_MICROS));
        rocksdbTableOpenIoHistogram = Metrics.register(factory.createMetricName("TableOpenIOMicros"),
                                                       HistogramUtils.createHistogram(cfs, stats, HistogramType.TABLE_OPEN_IO_MICROS));
        rocksdbMultiGetHistogram = Metrics.register(factory.createMetricName("MultiGet"),
                                                    HistogramUtils.createHistogram(cfs, stats, HistogramType.DB_MULTIGET));
        rocksdbReadBlockCompactionHistogram = Metrics.register(factory.createMetricName("ReadBlockCompactionMicros"),
                                                               HistogramUtils.createHistogram(cfs, stats, HistogramType.READ_BLOCK_COMPACTION_MICROS));
        rocksdbReadBlockGetHistogram = Metrics.register(factory.createMetricName("ReadBlockGetMicros"),
                                                        HistogramUtils.createHistogram(cfs, stats, HistogramType.READ_BLOCK_GET_MICROS));
        rocksdbWriteRawBlockHistogram = Metrics.register(factory.createMetricName("WriteRawBlockMicros"),
                                                         HistogramUtils.createHistogram(cfs, stats, HistogramType.WRITE_RAW_BLOCK_MICROS));
        rocksdbStallL0SlowdownHistogram = Metrics.register(factory.createMetricName("StallL0SlowdownCount"),
                                                           HistogramUtils.createHistogram(cfs, stats, HistogramType.STALL_L0_SLOWDOWN_COUNT));
        rocksdbMemtableCompactionHistogram = Metrics.register(factory.createMetricName("MemtableCompactionCount"),
                                                              HistogramUtils.createHistogram(cfs, stats, HistogramType.STALL_MEMTABLE_COMPACTION_COUNT));
        rocksdbStallL0NumFilesHistogram = Metrics.register(factory.createMetricName("StallL0NumFilesCount"),
                                                           HistogramUtils.createHistogram(cfs, stats, HistogramType.STALL_L0_NUM_FILES_COUNT));
        rocksdbHardRateLimitDelayHistogram = Metrics.register(factory.createMetricName("HardRateLimitDelayCount"),
                                                              HistogramUtils.createHistogram(cfs, stats, HistogramType.HARD_RATE_LIMIT_DELAY_COUNT));
        rocksdbSoftRateLimitDelayHistogram = Metrics.register(factory.createMetricName("SoftRateLimitDelayCount"),
                                                              HistogramUtils.createHistogram(cfs, stats, HistogramType.SOFT_RATE_LIMIT_DELAY_COUNT));
        rocksdbNumFilesInSingleCompactionHistogram = Metrics.register(factory.createMetricName("NumFilesInSingleCompaction"),
                                                                      HistogramUtils.createHistogram(cfs, stats, HistogramType.NUM_FILES_IN_SINGLE_COMPACTION));
        rocksdbDbSeekHistogram = Metrics.register(factory.createMetricName("DbSeek"),
                                                  HistogramUtils.createHistogram(cfs, stats, HistogramType.DB_SEEK));
        rocksdbWriteStallHistogram = Metrics.register(factory.createMetricName("WriteStall"),
                                         HistogramUtils.createHistogram(cfs, stats, HistogramType.WRITE_STALL));
        rocksdbSstReadMsHistogram = Metrics.register(factory.createMetricName("SstReadMs"),
                                                      HistogramUtils.createHistogram(cfs, stats, HistogramType.SST_READ_MICROS));
        rocksdbNumSubcompactionsScheduledHistogram = Metrics.register(factory.createMetricName("NumSubCompactionsScheduled"),
                                                     HistogramUtils.createHistogram(cfs, stats, HistogramType.NUM_SUBCOMPACTIONS_SCHEDULED));
        rocksdbBytesPerReadHistogram = Metrics.register(factory.createMetricName("BytesPerRead"),
                                                      HistogramUtils.createHistogram(cfs, stats, HistogramType.BYTES_PER_READ));
        rocksdbBytesPerWriteHistogram = Metrics.register(factory.createMetricName("BytesPerWrite"),
                                                      HistogramUtils.createHistogram(cfs, stats, HistogramType.BYTES_PER_WRITE));
        rocksdbBytesPerMultiGetHistogram = Metrics.register(factory.createMetricName("BytesPerMultiget"),
                                                      HistogramUtils.createHistogram(cfs, stats, HistogramType.BYTES_PER_MULTIGET));
        rocksdbBytesCompressedHistogram = Metrics.register(factory.createMetricName("BytesCompressed"),
                                                           HistogramUtils.createHistogram(cfs, stats, HistogramType.BYTES_COMPRESSED));
        rocksdbBytesDecompressedHistogram = Metrics.register(factory.createMetricName("BytesDecompressed"),
                                                      HistogramUtils.createHistogram(cfs, stats, HistogramType.BYTES_DECOMPRESSED));
        rocksdbCompressionTimeUsHistogram = Metrics.register(factory.createMetricName("CompressionTimeUs"),
                                                      HistogramUtils.createHistogram(cfs, stats, HistogramType.COMPRESSION_TIMES_NANOS));
        rocksdbDecompressionTimeUsHistogram = Metrics.register(factory.createMetricName("DecompressionTimeUs"),
                                                      HistogramUtils.createHistogram(cfs, stats, HistogramType.DECOMPRESSION_TIMES_NANOS));
        rocksdbReadNumMergeOperandsHistogram = Metrics.register(factory.createMetricName("ReadNumMergeOperands"),
                                                      HistogramUtils.createHistogram(cfs, stats, HistogramType.READ_NUM_MERGE_OPERANDS));
        rocksdbHistogramEnumMaxHistogram = Metrics.register(factory.createMetricName("HistogramEnumMaxHistogram"),
                                                      HistogramUtils.createHistogram(cfs, stats, HistogramType.HISTOGRAM_ENUM_MAX));
        rocksdbIngestTimeHistogram = Metrics.histogram(factory.createMetricName("IngestTime"), true);
        rocksdbIngestWaitTimeHistogram = Metrics.histogram(factory.createMetricName("IngestWaitTime"), true);

        rocksdbNumSstablePerLevel = new ArrayList<>(RocksDBConfigs.MAX_LEVELS);
        for (int level = 0; level < RocksDBConfigs.MAX_LEVELS; level ++)
        {
            final int fLevel = level;
            rocksdbNumSstablePerLevel.add(Metrics.register(factory.createMetricName("SSTableCountPerLevel." + fLevel),
                                                           new Gauge<Integer>(){
                                                               public Integer getValue()
                                                               {
                                                                   try
                                                                   {
                                                                       return RocksDBUtils.getNumberOfSstablesByLevel(RocksEngine.getRocksDBInstance(cfs), fLevel);
                                                                   } catch (Throwable e) {
                                                                       LOGGER.warn("Failed to get sstable count by level.", e);
                                                                       return 0;
                                                                   }
                                                               }
                                                           }));
        }

        rocksdbPendingCompactionBytes = Metrics.register(factory.createMetricName("PendingCompactionBytes"),
                                                        new Gauge<Long>(){
                                                            public Long getValue()
                                                            {
                                                                try
                                                                {
                                                                    return RocksDBUtils.getPendingCompactionBytes(RocksEngine.getRocksDBInstance(cfs));
                                                                } catch (Throwable e) {
                                                                    LOGGER.warn("Failed to get pending compaction bytes", e);
                                                                    return 0L;
                                                                }
                                                            }
                                                        });

        rocksdbIterMove = Metrics.counter(factory.createMetricName("RocksIterMove"));
        rocksdbIterSeek = Metrics.counter(factory.createMetricName("RocksIterSeek"));;
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
