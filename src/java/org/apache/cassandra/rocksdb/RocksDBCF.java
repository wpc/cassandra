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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.RocksdbTableMetrics;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CassandraCompactionFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

import static org.apache.cassandra.rocksdb.RocksEngine.ROCKSDB_DIR;

/**
 * A wrapper around RocksDB instance.
 */
public class RocksDBCF
{
    private final UUID cfID;
    private final RocksDB rocksDB;
    private final Statistics stats;
    private final RocksdbTableMetrics rocksMetrics;
    private final CassandraCompactionFilter compactionFilter;

    public RocksDBCF(ColumnFamilyStore cfs) throws RocksDBException
    {
        cfID = cfs.metadata.cfId;

        final long writeBufferSize = 8 * 512 * 1024 * 1024L;
        final long softPendingCompactionBytesLimit = 100 * 64 * 1073741824L;
        DBOptions dbOptions = new DBOptions();
        stats = new Statistics();
        stats.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        compactionFilter = new CassandraCompactionFilter(cfs.metadata.params.purgeTtlOnExpiration);

        dbOptions.setCreateIfMissing(true);
        dbOptions.setAllowConcurrentMemtableWrite(true);
        dbOptions.setEnableWriteThreadAdaptiveYield(true);
        dbOptions.setBytesPerSync(1024 * 1024);
        dbOptions.setWalBytesPerSync(1024 * 1024);
        dbOptions.setMaxBackgroundCompactions(20);
        dbOptions.setBaseBackgroundCompactions(20);
        dbOptions.setMaxSubcompactions(8);
        dbOptions.setStatistics(stats);

        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        columnFamilyOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        columnFamilyOptions.setWriteBufferSize(writeBufferSize);
        columnFamilyOptions.setMaxBytesForLevelBase(4 * writeBufferSize);
        columnFamilyOptions.setSoftPendingCompactionBytesLimit(softPendingCompactionBytesLimit);
        columnFamilyOptions.setHardPendingCompactionBytesLimit(8 * softPendingCompactionBytesLimit);
        columnFamilyOptions.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
        columnFamilyOptions.setMergeOperatorName("cassandra");
        columnFamilyOptions.setCompactionFilter(compactionFilter);

        final org.rocksdb.BloomFilter bloomFilter = new BloomFilter(10, false);
        final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setFilter(bloomFilter);
        columnFamilyOptions.setTableFormatConfig(tableOptions);

        ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions);

        String rocksDBTableDir = ROCKSDB_DIR + "/" + cfs.keyspace.getName() + "/" + cfs.name;
        FileUtils.createDirectory(ROCKSDB_DIR);
        FileUtils.createDirectory(rocksDBTableDir);
        rocksDB = RocksDB.open(dbOptions, rocksDBTableDir, Collections.singletonList(columnFamilyDescriptor), new ArrayList<>(1));

        rocksMetrics = new RocksdbTableMetrics(cfs, stats);
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

    public RocksdbTableMetrics getRocksMetrics()
    {
        return rocksMetrics;
    }
}
