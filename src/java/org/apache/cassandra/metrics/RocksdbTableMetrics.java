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

import com.codahale.metrics.Histogram;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.rocksdb.encoding.metrics.HistogramUtils;
import org.rocksdb.HistogramType;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class RocksdbTableMetrics
{
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
    public final Histogram rocksdbIngestTimeHistogram;

    public RocksdbTableMetrics(ColumnFamilyStore cfs) {
        factory = new RocksTableMetricNameFactory(cfs);

        rocksdbGetHistogram = Metrics.register(factory.createMetricName("Get"),
                                               HistogramUtils.createHistogram(cfs, HistogramType.DB_GET));
        rocksdbWritedHistogram = Metrics.register(factory.createMetricName("Write"),
                                                  HistogramUtils.createHistogram(cfs, HistogramType.DB_WRITE));
        rocksdbTableSyncHistogram = Metrics.register(factory.createMetricName("TableSyncMicros"),
                                                     HistogramUtils.createHistogram(cfs, HistogramType.TABLE_SYNC_MICROS));
        rocksdbCompactionOutFileSyncHistogram = Metrics.register(factory.createMetricName("CompactionOutFileSyncMicros"),
                                                                 HistogramUtils.createHistogram(cfs, HistogramType.COMPACTION_OUTFILE_SYNC_MICROS));
        rocksdbWalFileSyncHistogram = Metrics.register(factory.createMetricName("WalFileSyncMicros"),
                                                       HistogramUtils.createHistogram(cfs, HistogramType.WAL_FILE_SYNC_MICROS));
        rocksdbManifiestSyncHistogram = Metrics.register(factory.createMetricName("ManifiestSyncMicros"),
                                                         HistogramUtils.createHistogram(cfs, HistogramType.MANIFEST_FILE_SYNC_MICROS));
        rocksdbTableOpenIoHistogram = Metrics.register(factory.createMetricName("TableOpenIOMicros"),
                                                       HistogramUtils.createHistogram(cfs, HistogramType.TABLE_OPEN_IO_MICROS));
        rocksdbMultiGetHistogram = Metrics.register(factory.createMetricName("MultiGet"),
                                                    HistogramUtils.createHistogram(cfs, HistogramType.DB_MULTIGET));
        rocksdbReadBlockCompactionHistogram = Metrics.register(factory.createMetricName("ReadBlockCompactionMicros"),
                                                               HistogramUtils.createHistogram(cfs, HistogramType.READ_BLOCK_COMPACTION_MICROS));
        rocksdbReadBlockGetHistogram = Metrics.register(factory.createMetricName("ReadBlockGetMicros"),
                                                        HistogramUtils.createHistogram(cfs, HistogramType.READ_BLOCK_GET_MICROS));
        rocksdbWriteRawBlockHistogram = Metrics.register(factory.createMetricName("WriteRawBlockMicros"),
                                                         HistogramUtils.createHistogram(cfs, HistogramType.WRITE_RAW_BLOCK_MICROS));
        rocksdbStallL0SlowdownHistogram = Metrics.register(factory.createMetricName("StallL0SlowdownCount"),
                                                           HistogramUtils.createHistogram(cfs, HistogramType.STALL_L0_SLOWDOWN_COUNT));
        rocksdbMemtableCompactionHistogram = Metrics.register(factory.createMetricName("MemtableCompactionCount"),
                                                              HistogramUtils.createHistogram(cfs, HistogramType.STALL_MEMTABLE_COMPACTION_COUNT));
        rocksdbStallL0NumFilesHistogram = Metrics.register(factory.createMetricName("StallL0NumFilesCount"),
                                                           HistogramUtils.createHistogram(cfs, HistogramType.STALL_L0_NUM_FILES_COUNT));
        rocksdbHardRateLimitDelayHistogram = Metrics.register(factory.createMetricName("HardRateLimitDelayCount"),
                                                              HistogramUtils.createHistogram(cfs, HistogramType.HARD_RATE_LIMIT_DELAY_COUNT));
        rocksdbSoftRateLimitDelayHistogram = Metrics.register(factory.createMetricName("SoftRateLimitDelayCount"),
                                                              HistogramUtils.createHistogram(cfs, HistogramType.SOFT_RATE_LIMIT_DELAY_COUNT));
        rocksdbNumFilesInSingleCompactionHistogram = Metrics.register(factory.createMetricName("NumFilesInSingleCompaction"),
                                                                      HistogramUtils.createHistogram(cfs, HistogramType.NUM_FILES_IN_SINGLE_COMPACTION));
        rocksdbDbSeekHistogram = Metrics.register(factory.createMetricName("DbSeek"),
                                                  HistogramUtils.createHistogram(cfs, HistogramType.DB_SEEK));
        rocksdbWriteStallHistogram = Metrics.register(factory.createMetricName("WriteStall"),
                                                      HistogramUtils.createHistogram(cfs, HistogramType.WRITE_STALL));
        rocksdbIngestTimeHistogram = Metrics.histogram(factory.createMetricName("IngestTime"), true);
    }

    static class RocksTableMetricNameFactory implements MetricNameFactory
    {
        private static final String TYPE = "Rocksdb";
        private final String keyspaceName;
        private final String tableName;
        RocksTableMetricNameFactory(ColumnFamilyStore cfs)
        {
            this.keyspaceName = cfs.keyspace.getName();
            this.tableName = cfs.name;
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
