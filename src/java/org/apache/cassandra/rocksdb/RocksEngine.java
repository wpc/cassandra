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

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
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
import org.apache.cassandra.rocksdb.encoding.RowKeyEncoder;
import org.apache.cassandra.rocksdb.encoding.value.RowValueEncoder;
import org.apache.cassandra.rocksdb.streaming.RocksDBStreamReceiveTask;
import org.apache.cassandra.rocksdb.streaming.RocksDBStreamTransferTask;
import org.apache.cassandra.rocksdb.streaming.RocksDBStreamUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksEngine implements StorageEngine
{
    private static final Logger logger = LoggerFactory.getLogger(RocksEngine.class);

    public static final String DEFAULT_ROCKSDB_KEYSPACE = "rocksdb";
    public static final String DEFAULT_ROCKSDB_DIR = "/data/rocksdb";

    public static final String ROCKSDB_KEYSPACE = System.getProperty("cassandra.rocksdb.keyspace", DEFAULT_ROCKSDB_KEYSPACE);
    public static final String ROCKSDB_DIR = System.getProperty("cassandra.rocksdb.dir", DEFAULT_ROCKSDB_DIR);
    
    public final ConcurrentMap<UUID, RocksDBCF> rocksDBFamily = new ConcurrentHashMap<>();

    public void openColumnFamilyStore(ColumnFamilyStore cfs)
    {
        try
        {
            rocksDBFamily.putIfAbsent(cfs.metadata.cfId, new RocksDBCF(cfs));
        }
        catch (RocksDBException e)
        {
            e.printStackTrace();
        }
    }

    public void apply(ColumnFamilyStore cfs, PartitionUpdate update, boolean writeCommitLog)
    {
        DecoratedKey partitionKey = update.partitionKey();

        for (Row row : update)
        {
            applyRowToRocksDB(cfs, partitionKey, row);
        }

        Row staticRow = update.staticRow();
        if (!staticRow.isEmpty())
        {
            applyRowToRocksDB(cfs, partitionKey, staticRow);
        }
    }

    public UnfilteredRowIterator queryStorage(ColumnFamilyStore cfs, SinglePartitionReadCommand readCommand)
    {
        Partition partition = new RocksDBPartition(rocksDBFamily.get(cfs.metadata.cfId).getRocksDB(),
                                                   readCommand.partitionKey(),
                                                   readCommand.metadata());
        return readCommand.clusteringIndexFilter().getUnfilteredRowIterator(readCommand.columnFilter(), partition);
    }

    public AbstractStreamTransferTask getStreamTransferTask(StreamSession session,
                                                            UUID cfId,
                                                            Collection<Range<Token>> ranges)
    {
        RocksDBStreamTransferTask task = new RocksDBStreamTransferTask(session, cfId);
        task.addTransferRocksdbFile(cfId,
                                    RocksEngine.getRocksDBInstance(cfId),
                                    ranges);
        return task;
    }

    public AbstractStreamReceiveTask getStreamReceiveTask(StreamSession session, StreamSummary summary)
    {
        return new RocksDBStreamReceiveTask(session, summary.cfId, summary.files, summary.totalSize);
    }

    private void applyRowToRocksDB(ColumnFamilyStore cfs,
                                   DecoratedKey partitionKey,
                                   Row row)
    {

        Clustering clustering = row.clustering();

        byte[] rocksDBKey = RowKeyEncoder.encode(partitionKey, clustering, cfs.metadata);
        byte[] rocksDBValue = RowValueEncoder.encode(cfs.metadata, row);

        // value colummns
        try
        {
            rocksDBFamily.get(cfs.metadata.cfId).getRocksDB().merge(rocksDBKey, rocksDBValue);
        }
        catch (RocksDBException e)
        {
            logger.error(e.toString(), e);
        }
    }

    public static RocksDB getRocksDBInstance(ColumnFamilyStore cfs)
    {
        if (cfs.engine instanceof RocksEngine)
        {
            return ((RocksEngine) cfs.engine).rocksDBFamily.get(cfs.metadata.cfId).getRocksDB();
        }
        return null;
    }

    public static RocksDB getRocksDBInstance(UUID cfId)
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(cfId);
        if (cfs != null && cfs.engine instanceof RocksEngine)
        {
            return ((RocksEngine) cfs.engine).rocksDBFamily.get(cfId).getRocksDB();
        }
        return null;
    }

    public static RocksDBCF getRocksDBCF(UUID cfId)
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(cfId);
        if (cfs != null && cfs.engine instanceof RocksEngine)
        {
            return ((RocksEngine) cfs.engine).rocksDBFamily.get(cfId);
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
        RocksDB db = rocksDBFamily.get(cfs.metadata.cfId).getRocksDB();
        for (Range range : completeRanges)
        {
            try
            {
                deleteRange(db, range);
            } catch (RocksDBException e)
            {
                logger.error("Cleanup failed hitting a rocksdb exception", e);
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    public void deleteRange(RocksDB db, Range<Token> range) throws RocksDBException
    {
        db.deleteRange(RowKeyEncoder.encodeToken(range.left), RowKeyEncoder.encodeToken(range.right));
    }
}
