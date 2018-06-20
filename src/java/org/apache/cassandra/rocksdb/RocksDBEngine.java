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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.engine.StorageEngine;
import org.apache.cassandra.engine.streaming.AbstractStreamReceiveTask;
import org.apache.cassandra.engine.streaming.AbstractStreamTransferTask;
import org.apache.cassandra.exceptions.StorageEngineException;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.metrics.SecondaryIndexMetrics;
import org.apache.cassandra.rocksdb.encoding.PartitionMetaEncoder;
import org.apache.cassandra.rocksdb.encoding.RowKeyEncoder;
import org.apache.cassandra.rocksdb.encoding.value.RowValueEncoder;
import org.apache.cassandra.rocksdb.streaming.RocksDBStreamReceiveTask;
import org.apache.cassandra.rocksdb.streaming.RocksDBStreamTransferTask;
import org.apache.cassandra.rocksdb.streaming.RocksDBStreamUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBEngine implements StorageEngine
{
    private static final Logger logger = LoggerFactory.getLogger(RocksDBEngine.class);
    private static final ExecutorService FLUSH_EXECUTOR = new JMXEnabledThreadPoolExecutor(RocksDBConfigs.FLUSH_CONCURRENCY,
                                                                                           StageManager.KEEPALIVE,
                                                                                           TimeUnit.SECONDS,
                                                                                           new LinkedBlockingQueue<Runnable>(),
                                                                                           new NamedThreadFactory("RocksDBFlush"),
                                                                                           "internal");

    public final ConcurrentMap<Pair<UUID, String>, RocksDBCF> rocksDBFamily = new ConcurrentHashMap<>();

    static
    {
        RocksDB.loadLibrary();
    }

    public final RateLimiter rateLimiter = new RateLimiter(1024L * 1024L *
                                                           verifyCompactionThroughputInBounds(
                                                           DatabaseDescriptor.getCompactionThroughputMbPerSec()));
    public final Cache cache = new LRUCache(RocksDBConfigs.BLOCK_CACHE_SIZE_MBYTES * 1024 * 1024L);
    public final Cache metaCache = new LRUCache(RocksDBConfigs.META_BLOCK_CACHE_SIZE_MBYTES * 1024 * 1024L);

    public static SecondaryIndexMetrics secondaryIndexMetrics = new SecondaryIndexMetrics();

    private final Keyspace keyspace;

    public RocksDBEngine(Keyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    public static int verifyCompactionThroughputInBounds(int value)
    {
        if (value == 0)
        {
            return Integer.MAX_VALUE;
        }
        return value;
    }

    public void openColumnFamilyStore(ColumnFamilyStore cfs)
    {
        Pair<UUID, String> key = new Pair<>(cfs.metadata.cfId, cfs.name);
        if(rocksDBFamily.containsKey(key)) {
            return;
        }
        try
        {

            rocksDBFamily.put(key, new RocksDBCF(cfs)); // putIfAbsent
        }
        catch (RocksDBException e)
        {
            e.printStackTrace();
        }
    }

    public void apply(ColumnFamilyStore cfs, PartitionUpdate update, UpdateTransaction indexer, boolean writeCommitLog)
    {
        DecoratedKey partitionKey = update.partitionKey();

        DeletionTime partitionLevelDeletion = update.partitionLevelDeletion();
        if (!partitionLevelDeletion.isLive())
        {
            applyPartitionLevelDeletionToRocksdb(cfs, partitionKey, partitionLevelDeletion);
        }

        for (Row row : update)
        {
            applyRowToRocksDB(cfs, writeCommitLog, partitionKey, indexer, row);
        }

        Row staticRow = update.staticRow();
        if (!staticRow.isEmpty())
        {
            applyRowToRocksDB(cfs, writeCommitLog, partitionKey, indexer, staticRow);
        }
    }

    private void applyPartitionLevelDeletionToRocksdb(ColumnFamilyStore cfs, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion)
    {
        byte[] rocksDBKey = RowKeyEncoder.encode(partitionKey, cfs.metadata);
        byte[] rocksDBValue = PartitionMetaEncoder.encodePartitionLevelDeletion(partitionLevelDeletion);

        try
        {
            rocksDBFamily.get(new Pair<>(cfs.metadata.cfId, cfs.name)).merge(RocksCFName.META, partitionKey, rocksDBKey, rocksDBValue);
        }
        catch (RocksDBException e)
        {
            logger.error(e.toString(), e);
            throw new StorageEngineException("Row merge failed", e);
        }
    }

    public UnfilteredRowIterator queryStorage(ColumnFamilyStore cfs, SinglePartitionReadCommand readCommand)
    {
        Partition partition = new RocksDBPartition(rocksDBFamily.get(new Pair<>(cfs.metadata.cfId, cfs.name)),
                                                   readCommand.partitionKey(),
                                                   readCommand.metadata());
        return readCommand.clusteringIndexFilter().getUnfilteredRowIterator(readCommand.columnFilter(), partition);
    }

    public Future<Void> forceFlush(ColumnFamilyStore cfs)
    {
        FutureTask<Void> task = ListenableFutureTask.create(new Callable<Void>()
        {
            public Void call()
            {
                try
                {
                    RocksDBCF rocksDBCF = getRocksDBCF(cfs);

                    if (rocksDBCF != null)
                        rocksDBCF.forceFlush();
                    else
                        logger.info("Can not find rocksdb table: " + cfs.name);
                }
                catch (RocksDBException e)
                {
                    logger.error("Failed to flush Rocksdb table: " + cfs.name, e);
                }
                return null;
            }
        });
        FLUSH_EXECUTOR.execute(task);
        return task;
    }

    public void truncate(ColumnFamilyStore cfs)
    {
        try
        {
            RocksDBCF rocksDBCF = getRocksDBCF(cfs);

            if (rocksDBCF != null)
                rocksDBCF.truncate();
            else
                logger.info("Can not find rocksdb table: " + cfs.name);
        }
        catch (RocksDBException e)
        {
            logger.error(e.toString(), e);
        }
    }

    public void close(ColumnFamilyStore cfs)
    {
        RocksDBCF rocksDBCF = getRocksDBCF(cfs);
        if (rocksDBCF != null)
        {
            rocksDBCF.close();
        }
        else
            logger.info("Can not find rocksdb table: " + cfs.name);
    }

    public void setCompactionThroughputMbPerSec(int throughputMbPerSec)
    {
        throughputMbPerSec = verifyCompactionThroughputInBounds(throughputMbPerSec);
        rateLimiter.setBytesPerSecond(1024L * 1024L * throughputMbPerSec);
        logger.info("Change keyspace " + keyspace.getName() +
                    " compaction throughput MB per sec to " + throughputMbPerSec);
    }

    public AbstractStreamTransferTask getStreamTransferTask(StreamSession session,
                                                            UUID cfId,
                                                            Collection<Range<Token>> ranges)
    {
        RocksDBStreamTransferTask task = new RocksDBStreamTransferTask(session, cfId);
        task.addTransferRocksdbFile(cfId,
                                    RocksDBEngine.getRocksDBCF(cfId),
                                    ranges);
        return task;
    }

    public AbstractStreamReceiveTask getStreamReceiveTask(StreamSession session, StreamSummary summary)
    {
        return new RocksDBStreamReceiveTask(session, summary.cfId, summary.files, summary.totalSize);
    }

    private void applyRowToRocksDB(ColumnFamilyStore cfs,
                                   boolean writeCommitLog,
                                   DecoratedKey partitionKey,
                                   UpdateTransaction indexer,
                                   Row row)
    {

        Clustering clustering = row.clustering();

        byte[] rocksDBKey = RowKeyEncoder.encode(partitionKey, clustering, cfs.metadata);
        byte[] rocksDBValue = RowValueEncoder.encode(cfs.metadata, row);

        try
        {
            indexer.start();
            rocksDBFamily.get(new Pair<>(cfs.metadata.cfId, cfs.name)).merge(partitionKey, rocksDBKey, rocksDBValue);
            if (indexer != UpdateTransaction.NO_OP)
            {
                try
                {
                    secondaryIndexMetrics.rsiTotalInsertions.inc();
                    indexer.onInserted(row);
                }
                catch (RuntimeException e)
                {
                    secondaryIndexMetrics.rsiInsertionFailures.inc();
                    logger.error(e.toString(), e);
                    throw new StorageEngineException("Index update failed", e);
                }
            }
        }
        catch (RocksDBException e)
        {
            logger.error(e.toString(), e);
            throw new StorageEngineException("Row merge failed", e);
        }
        finally
        {
            indexer.commit();
        }
    }

    public static RocksDBCF getRocksDBCF(UUID cfId)
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(cfId);
        return getRocksDBCF(cfs);
    }

    public static RocksDBCF getRocksDBCF(final ColumnFamilyStore cfs)
    {
        if (cfs != null && cfs.engine instanceof RocksDBEngine)
        {
            return ((RocksDBEngine) cfs.engine).rocksDBFamily.get(new Pair<>(cfs.metadata.cfId, cfs.name));
        }
        return null;
    }

    @Override
    public boolean cleanUpRanges(ColumnFamilyStore cfs)
    {
        Keyspace keyspace = cfs.keyspace;
        if (!StorageService.instance.isJoined())
        {
            logger.info("Cleanup cannot run before a node has joined the ring");
            return false;
        }
        final Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
        final Collection<Range<Token>> completeRanges = RocksDBStreamUtils.calcluateComplementRanges(cfs.getPartitioner(), ranges);
        RocksDBCF db = rocksDBFamily.get(new Pair<>(cfs.metadata.cfId, cfs.name));
        for (Range range : completeRanges)
        {
            try
            {
                deleteRange(db, range);
            }
            catch (RocksDBException e)
            {
                logger.error("Cleanup failed hitting a rocksdb exception", e);
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    public void deleteRange(RocksDBCF db, Range<Token> range) throws RocksDBException
    {
        db.deleteRange(RowKeyEncoder.encodeToken(range.left), RowKeyEncoder.encodeToken(range.right));
    }

    @Override
    public boolean doubleWrite()
    {
        return RocksDBConfigs.ROCKSDB_DOUBLE_WRITE;
    }

    @Override
    public long load()
    {
        long result = 0;
        for (RocksDBCF cf : rocksDBFamily.values())
        {
            try
            {
                result += RocksDBProperty.getEstimatedLiveDataSize(cf);
            }
            catch (RocksDBException e)
            {
                logger.warn("Failed to query live data size.", e);
            }
        }
        return result;
    }

    @Override
    public String dumpPartition(ColumnFamilyStore cfs, String partitionKey, int limit)
    {
        DecoratedKey decoratedKey = parseStringPartitionKey(cfs, partitionKey);
        byte[] rocksKeyPrefix = RowKeyEncoder.encode(decoratedKey, cfs.metadata);
        RocksDBCF rocksDBCF = getRocksDBCF(cfs.metadata.cfId);
        return rocksDBCF.dumpPrefix(decoratedKey, rocksKeyPrefix, limit);
    }

    private DecoratedKey parseStringPartitionKey(ColumnFamilyStore cfs, String partitionKey)
    {
        List<ColumnDefinition> keyColumns = cfs.metadata.partitionKeyColumns();
        if (keyColumns.size() > 1)
        {
            throw new UnsupportedOperationException("Composite partition key is not supported");
        }

        ByteBuffer keyBytes = Terms.asBytes(cfs.keyspace.getName(), partitionKey, keyColumns.get(0).type);
        return cfs.metadata.partitioner.decorateKey(keyBytes);
    }

    public String dumpPartitionMetaData(ColumnFamilyStore cfs, String partitionKey) throws RocksDBException
    {
        DecoratedKey decoratedKey = parseStringPartitionKey(cfs, partitionKey);
        byte[] rocksKey = RowKeyEncoder.encode(decoratedKey, cfs.metadata);
        RocksDBCF rocksDBCF = getRocksDBCF(cfs.metadata.cfId);
        byte[] meta = rocksDBCF.get(RocksCFName.META, decoratedKey, rocksKey);
        if (meta == null) {
            return "";
        }
        return "0x" + Hex.bytesToHex(rocksKey) + "\t0x" + Hex.bytesToHex(meta);
    }

    public void forceMajorCompaction(ColumnFamilyStore cfs)
    {
        RocksDBCF rocksDBCF = getRocksDBCF(cfs.metadata.cfId);
        try
        {
            rocksDBCF.compactRange();
        }
        catch (RocksDBException e)
        {
            logger.error(e.toString(), e);
            throw new RuntimeException(e);
        }
    }
}
