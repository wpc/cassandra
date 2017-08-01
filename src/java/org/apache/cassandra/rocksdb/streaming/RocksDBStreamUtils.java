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

package org.apache.cassandra.rocksdb.streaming;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.NotImplementedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.rocksdb.RocksDBCF;
import org.apache.cassandra.rocksdb.RocksEngine;
import org.apache.cassandra.utils.Pair;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBStreamUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStreamWriter.class);
    // To prevent write stall, only ingest streamed sstable when number of level0 sstables small or equal than this threshold.
    private static final int INGESTION_NUM_FILES_AT_LEVEL0_THRESHOLD = Integer.getInteger("cassandra.rocksdb.num_files_at_level0_threshold", 10);
    private static final long INGESTION_WAIT_MS = Long.getLong("cassandra.rocksdb.ingestion_wait_ms", 5000);
    public static final byte[] EOF = new byte[]{'\0'};
    public static final byte[] MORE = new byte[]{'1'};

    public static ColumnFamilyStore getColumnFamilyStore(UUID cfId)
    {
        Pair<String, String> kscf = Schema.instance.getCF(cfId);
        ColumnFamilyStore cfs = null;
        if (kscf != null)
            cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

        if (kscf == null || cfs == null)
        {
            LOGGER.warn("CF " + cfId + " was dropped during streaming");
        }
        return cfs;
    }

    public static RocksDB getRocksdb(UUID cfId)
    {
        ColumnFamilyStore cfs = getColumnFamilyStore(cfId);
        if (cfs == null)
            return null;
        return RocksEngine.getRocksDBInstance(cfs);
    }

    public static int getNumberOfLevel0Sstables(RocksDB db) throws RocksDBException
    {
        return Integer.parseInt(db.getProperty("rocksdb.num-files-at-level0"));
    }

    public static void ingestRocksSstable(UUID cfId, String sstFile) throws RocksDBException
    {
        ColumnFamilyStore cfs = getColumnFamilyStore(cfId);
        RocksDBCF rocksDBCF = RocksEngine.getRocksDBCF(cfId);
        if (cfs == null || rocksDBCF == null)
            return;

        RocksDB db = rocksDBCF.getRocksDB();

        // There might be multiple streaming sessions (threads) for the same sstable/db at the same time.
        // Adding lock to the db to prevent multiple sstables are ingested at same time and trigger
        // write stalls.
        synchronized (db)
        {
            // Wait until compaction catch up by examing the number of l0 sstables.
            while (true)
            {
                int numOfLevel0Sstables = getNumberOfLevel0Sstables(db);
                if (numOfLevel0Sstables <= INGESTION_NUM_FILES_AT_LEVEL0_THRESHOLD)
                    break;
                try
                {
                    LOGGER.debug("Number of level0 sstables " + numOfLevel0Sstables + " exceeds the threshold " + INGESTION_NUM_FILES_AT_LEVEL0_THRESHOLD
                                + ", sleep for " + INGESTION_WAIT_MS + "ms.");
                    Thread.sleep(INGESTION_WAIT_MS);
                }
                catch (InterruptedException e)
                {
                    LOGGER.warn("Ingestion wait interrupted, procceding.");
                }
            }

            long startTime = System.currentTimeMillis();
            try(final IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions()) {
                db.ingestExternalFile(Arrays.asList(sstFile), ingestExternalFileOptions);
            }
            rocksDBCF.getRocksMetrics().rocksdbIngestTimeHistogram.update(System.currentTimeMillis() - startTime);
        }
    }

    /**
     * While streaming, the ranges is sorted by range.left and deoverlaped. For the last
     * range in ranges, there is a possibility that  MinToken < range.right < range.left < MaxToken
     * as this is a valid range in consistent hash ring. Hoewever rocksdb writer can't deal this
     * case as it requires key value pairs streamed in order, so we need to normalize this range by
     * split it into [right, MaxToken] [MinToken, left], and insert back to ranges and keep the ranges
     * order by range.left.
     */
    public static Collection<Range<Token>> normalizeRanges(Collection<Range<Token>> ranges)
    {
        if (ranges.isEmpty())
            return ranges;

        List<Range<Token>> normalized = new ArrayList<>(ranges);
        Range<Token> last = normalized.get(normalized.size() - 1);
        if (last.left.compareTo(last.right) <= 0)
            return ranges;

        Token maxToken = getMaxToken(last.left.getPartitioner());
        Token minToken = getMinToken(last.left.getPartitioner());

        normalized.remove(normalized.get(normalized.size() - 1));
        normalized.add(new Range<>(last.left, maxToken));
        normalized.add(0, new Range<>(minToken, last.right));
        return normalized;
    }

    /**
     * Calcuate the complement of a given ranges (all the tokens not in the given range) in the token ring.
     */
    public static Collection<Range<Token>> calcluateComplementRanges(IPartitioner partitioner, Collection<Range<Token>> ranges)
    {
        Collection<Range<Token>> normalized = normalizeRanges(Range.normalize(ranges));

        ArrayList<Range<Token>> result = new ArrayList<>(ranges.size() + 1);

        Token start = getMinToken(partitioner);
        for (Range<Token> range : normalized)
        {
            Token end = range.left;
            if (!start.equals(end))
            {
                result.add(new Range<>(start, end));
            }
            start = range.right;
        }

        Token maxToken = getMaxToken(partitioner);
        if (!start.equals(maxToken))
        {
            result.add(new Range<>(start, maxToken));
        }
        return result;
    }

    public static Token getMaxToken(IPartitioner partitioner)
    {
        if (partitioner instanceof Murmur3Partitioner)
        {
            return new Murmur3Partitioner.LongToken(Murmur3Partitioner.MAXIMUM);
        } else if (partitioner instanceof RandomPartitioner)
        {
            return new RandomPartitioner.BigIntegerToken(RandomPartitioner.MAXIMUM);
        }
        throw new NotImplementedException(partitioner.getClass().getName() + "is not supported");
    }

    public static Token getMinToken(IPartitioner partitioner)
    {
        if (partitioner instanceof Murmur3Partitioner)
        {
            return Murmur3Partitioner.MINIMUM;
        } else if (partitioner instanceof RandomPartitioner)
        {
            return RandomPartitioner.MINIMUM;
        }
        throw new NotImplementedException(partitioner.getClass().getName() + "is not supported");
    }

    public static String toString(Cell cell)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Cell tombstone:").append(cell.isTombstone())
            .append(", expiring:").append(cell.isExpiring())
            .append(", timestamp:").append(cell.timestamp())
            .append(", ttl:").append(cell.ttl())
            .append(", value:").append(Hex.encodeHex(cell.value().array()));
        return sb.toString();
    }
}
