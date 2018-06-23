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
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.RocksDBTableMetrics;
import org.apache.cassandra.rocksdb.encoding.RowKeyEncoder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CassandraCompactionFilter;
import org.rocksdb.CassandraPartitionMetaMergeOperator;
import org.rocksdb.CassandraValueMergeOperator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;
import org.rocksdb.Statistics;
import org.rocksdb.WriteOptions;

import static org.apache.cassandra.rocksdb.RocksDBConfigs.MERGE_OPERANDS_LIMIT;

/*
 * Holds the rocksdb instance and cfs for single shard instance
 */
public class RocksDBInstanceHandle
{
    private static final Logger logger = LoggerFactory.getLogger(RocksDBInstanceHandle.class);

    private final RocksDB rocksDB;
    private final ColumnFamilyHandle metaCfHandle;
    private final ColumnFamilyHandle dataCfHandle;
    private final ColumnFamilyHandle indexCfHandle;
    private final CassandraCompactionFilter compactionFilter;

    private FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true);

    private final CassandraValueMergeOperator mergeOperator;
    private final CassandraPartitionMetaMergeOperator partitionMetaMergeOperator;

    public RocksDBInstanceHandle(ColumnFamilyStore cfs,
                                 String rocksDBTableDir,
                                 BlockBasedTableConfig tableOptions,
                                 BlockBasedTableConfig metaTableOptions,
                                 Statistics stats) throws RocksDBException
    {
        int gcGraceSeconds = cfs.metadata.params.gcGraceSeconds;
        boolean purgeTtlOnExpiration = cfs.metadata.params.purgeTtlOnExpiration;
        mergeOperator = new CassandraValueMergeOperator(gcGraceSeconds, MERGE_OPERANDS_LIMIT);
        partitionMetaMergeOperator = new CassandraPartitionMetaMergeOperator();

        // holding reference avoid compaction filter instance get gc
        this.compactionFilter = createCassandraCompactionFilter(cfs, purgeTtlOnExpiration, gcGraceSeconds);

        DBOptions dbOptions = new DBOptions();
        SstFileManager sstFileManager = new SstFileManager(Env.getDefault());

        final long writeBufferSize = RocksDBConfigs.WRITE_BUFFER_SIZE_MBYTES * 1024 * 1024L;
        final long softPendingCompactionBytesLimit = 64 * 1073741824L; //64G

        // sstFilemanager options
        sstFileManager.setDeleteRateBytesPerSecond(RocksDBConfigs.DELETE_RATE_BYTES_PER_SECOND);

        // db options
        dbOptions.setCreateIfMissing(true);
        dbOptions.setCreateMissingColumnFamilies(true);
        dbOptions.setAllowConcurrentMemtableWrite(true);
        dbOptions.setEnableWriteThreadAdaptiveYield(true);
        dbOptions.setBytesPerSync(1024 * 1024);
        dbOptions.setWalBytesPerSync(1024 * 1024);
        dbOptions.setMaxBackgroundCompactions(RocksDBConfigs.BACKGROUD_COMPACTIONS);
        dbOptions.setBaseBackgroundCompactions(RocksDBConfigs.BACKGROUD_COMPACTIONS);
        dbOptions.setMaxBackgroundFlushes(4);
        dbOptions.setMaxSubcompactions(8);
        dbOptions.setStatistics(stats);
        dbOptions.setRateLimiter(((RocksDBEngine) cfs.engine).rateLimiter);
        dbOptions.setSstFileManager(sstFileManager);
        dbOptions.setMaxTotalWalSize(RocksDBConfigs.MAX_TOTAL_WAL_SIZE_MBYTES * 1024 * 1024L);

        List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>(3);
        ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(3);

        // config meta column family
        ColumnFamilyOptions metaCfOptions = new ColumnFamilyOptions();
        metaCfOptions.setNumLevels(RocksDBConfigs.META_CF_MAX_LEVELS);
        metaCfOptions.setCompressionType(RocksDBConfigs.COMPRESSION_TYPE);
        metaCfOptions.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
        metaCfOptions.setMergeOperator(partitionMetaMergeOperator);
        metaCfOptions.setMaxWriteBufferNumber(2);
        metaCfOptions.setWriteBufferSize(RocksDBConfigs.META_WRITE_BUFFER_SIZE_MBYTES * 1024 * 1024L);
        metaCfOptions.setTableFormatConfig(metaTableOptions);
        ColumnFamilyDescriptor metaCfDescriptor = new ColumnFamilyDescriptor("meta".getBytes(), metaCfOptions);

        // config index column family
        ColumnFamilyOptions indexCfOptions = new ColumnFamilyOptions();
        indexCfOptions.setNumLevels(RocksDBConfigs.MAX_LEVELS);
        indexCfOptions.setCompressionType(RocksDBConfigs.COMPRESSION_TYPE);
        indexCfOptions.setBottommostCompressionType(RocksDBConfigs.BOTTOMMOST_COMPRESSION);
        indexCfOptions.setWriteBufferSize(RocksDBConfigs.INDEX_WRITE_BUFFER_SIZE_MBYTES * 1024 * 1024L);
        indexCfOptions.setMaxWriteBufferNumber(2);
        indexCfOptions.setMaxBytesForLevelBase(RocksDBConfigs.MAX_MBYTES_FOR_LEVEL_BASE * 1024 * 1024L);
        indexCfOptions.setSoftPendingCompactionBytesLimit(softPendingCompactionBytesLimit);
        indexCfOptions.setHardPendingCompactionBytesLimit(8 * softPendingCompactionBytesLimit);
        indexCfOptions.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
        indexCfOptions.setLevel0SlowdownWritesTrigger(RocksDBConfigs.LEVEL0_STOP_WRITES_TRIGGER);
        indexCfOptions.setLevel0StopWritesTrigger(RocksDBConfigs.LEVEL0_STOP_WRITES_TRIGGER);
        indexCfOptions.setLevelCompactionDynamicLevelBytes(!RocksDBConfigs.DYNAMIC_LEVEL_BYTES_DISABLED);
        indexCfOptions.setMergeOperator(mergeOperator);
        indexCfOptions.setCompactionFilter(this.compactionFilter);
        indexCfOptions.setTableFormatConfig(tableOptions);
        ColumnFamilyDescriptor indexCfDescriptor = new ColumnFamilyDescriptor("index".getBytes(), indexCfOptions);

        // config default column family for holding data
        ColumnFamilyOptions dataCfOptions = new ColumnFamilyOptions();
        dataCfOptions.setNumLevels(RocksDBConfigs.MAX_LEVELS);
        dataCfOptions.setCompressionType(RocksDBConfigs.COMPRESSION_TYPE);
        dataCfOptions.setBottommostCompressionType(RocksDBConfigs.BOTTOMMOST_COMPRESSION);
        dataCfOptions.setWriteBufferSize(writeBufferSize);
        dataCfOptions.setMaxWriteBufferNumber(4);
        dataCfOptions.setMaxBytesForLevelBase(RocksDBConfigs.MAX_MBYTES_FOR_LEVEL_BASE * 1024 * 1024L);
        dataCfOptions.setSoftPendingCompactionBytesLimit(softPendingCompactionBytesLimit);
        dataCfOptions.setHardPendingCompactionBytesLimit(8 * softPendingCompactionBytesLimit);
        dataCfOptions.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
        dataCfOptions.setLevel0SlowdownWritesTrigger(RocksDBConfigs.LEVEL0_STOP_WRITES_TRIGGER);
        dataCfOptions.setLevel0StopWritesTrigger(RocksDBConfigs.LEVEL0_STOP_WRITES_TRIGGER);
        dataCfOptions.setLevelCompactionDynamicLevelBytes(!RocksDBConfigs.DYNAMIC_LEVEL_BYTES_DISABLED);
        dataCfOptions.setMergeOperator(mergeOperator);
        dataCfOptions.setCompactionFilter(this.compactionFilter);
        dataCfOptions.setTableFormatConfig(tableOptions);
        ColumnFamilyDescriptor dataCfDescriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, dataCfOptions);

        cfDescs.add(dataCfDescriptor);
        cfDescs.add(metaCfDescriptor);
        cfDescs.add(indexCfDescriptor);

        this.rocksDB = RocksDB.open(dbOptions, rocksDBTableDir, cfDescs, columnFamilyHandles);

        assert columnFamilyHandles.size() == 3;
        this.dataCfHandle = columnFamilyHandles.get(0);
        this.metaCfHandle = columnFamilyHandles.get(1);
        this.indexCfHandle = columnFamilyHandles.get(2);

        this.compactionFilter.setMetaCfHandle(rocksDB, metaCfHandle);

        logger.info("Open rocksdb instance for cf {}.{} with path:{}, gcGraceSeconds:{}, purgeTTL:{}",
                    cfs.keyspace.getName(), cfs.name, rocksDBTableDir,
                    gcGraceSeconds, purgeTtlOnExpiration);
    }

    private CassandraCompactionFilter createCassandraCompactionFilter(ColumnFamilyStore cfs, boolean purgeTtlOnExpiration, int gcGraceSeconds)
    {
        Integer partitionKeyLength = RowKeyEncoder.calculateEncodedPartitionKeyLength(cfs.metadata);

        if (partitionKeyLength == null)
        {
            // partition key length is not fixed, compaction filter falls back to range scan for find partition meta data.
            return new CassandraCompactionFilter(purgeTtlOnExpiration, true, gcGraceSeconds);
        }
        else
        {
            // partition key has fix length, compaction filter will use point lookup with partition key for loading
            // parition meta data to save cpu
            return new CassandraCompactionFilter(purgeTtlOnExpiration, true, gcGraceSeconds, partitionKeyLength);
        }
    }

    private ColumnFamilyHandle getCfHandle(RocksCFName rocksCFName)
    {
        if(rocksCFName == RocksCFName.DEFAULT) {
            return dataCfHandle;
        }

        if (rocksCFName == RocksCFName.META) {
            return metaCfHandle;
        }

        if (rocksCFName == RocksCFName.INDEX) {
            return indexCfHandle;
        }

        throw new AssertionError("should not reach here");
    }

    public void merge(RocksCFName rocksCFName, WriteOptions writeOptions, byte[] key, byte[] value) throws RocksDBException
    {

        ColumnFamilyHandle cfHandle = getCfHandle(rocksCFName);
        rocksDB.merge(cfHandle, writeOptions, key, value);
    }


    public byte[] get(RocksCFName rocksCFName, ReadOptions readOptions, byte[] key) throws RocksDBException
    {
        ColumnFamilyHandle cfHandle = getCfHandle(rocksCFName);
        return rocksDB.get(cfHandle, readOptions, key);
    }

    public RocksDBIteratorAdapter newIterator(RocksCFName rocksCFName, ReadOptions options, RocksDBTableMetrics rocksMetrics)
    {
        rocksMetrics.rocksDBIterNew.inc();
        ColumnFamilyHandle cfHandle = getCfHandle(rocksCFName);
        return new RocksDBIteratorAdapter(rocksDB.newIterator(cfHandle, options), rocksMetrics);
    }

    public RocksDBIteratorAdapter newShardIterator(RocksCFName rocksCFName, ReadOptions options, RocksDBTableMetrics rocksMetrics)
    {
        rocksMetrics.rocksDBIterNew.inc();
        return new RocksDBIteratorAdapter(rocksDB.newIterator(getCfHandle(rocksCFName), options), rocksMetrics);
    }

    public void deleteRange(byte[] start, byte[] end) throws RocksDBException
    {
        rocksDB.deleteFilesInRange(start, end); //todo: make deleteFilesInRange API support cf_handles
        rocksDB.deleteRange(dataCfHandle, start, end);
        rocksDB.deleteRange(metaCfHandle, start, end);
        rocksDB.deleteRange(indexCfHandle, start, end);
    }


    public void compactRange() throws RocksDBException
    {
        rocksDB.compactRange(metaCfHandle);
        rocksDB.compactRange(dataCfHandle);
        rocksDB.compactRange(indexCfHandle);
    }

    public void forceFlush() throws RocksDBException
    {
        rocksDB.flush(flushOptions, metaCfHandle);
        rocksDB.flush(flushOptions, dataCfHandle);
        rocksDB.flush(flushOptions, indexCfHandle);
    }

    public String getProperty(RocksCFName rocksCFName, String property) throws RocksDBException
    {
        ColumnFamilyHandle cfHandle = getCfHandle(rocksCFName);
        return rocksDB.getProperty(cfHandle, property);
    }

    public void truncate(byte[] startRange, byte[] endRange) throws RocksDBException
    {
        // delete all sstables other than L0
        rocksDB.deleteFilesInRange(startRange, endRange);

        // Move L0 sstables to L1
        rocksDB.flush(flushOptions);
        rocksDB.compactRange();

        // delete all sstables other than L0
        rocksDB.deleteFilesInRange(startRange, endRange);
    }

    public void close()
    {
        rocksDB.close();
    }

    public void ingestRocksSstable(RocksCFName rocksCFName, String sstFile) throws RocksDBException
    {
        ColumnFamilyHandle cfhandle = getCfHandle(rocksCFName);
        try(final IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions()) {
            rocksDB.ingestExternalFile(cfhandle, Arrays.asList(sstFile), ingestExternalFileOptions);
        }
    }
}
