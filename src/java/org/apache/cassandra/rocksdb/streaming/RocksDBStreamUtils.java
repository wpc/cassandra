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


import java.math.BigInteger;
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
import org.apache.cassandra.rocksdb.RocksDBConfigs;
import org.apache.cassandra.rocksdb.RocksDBUtils;
import org.apache.cassandra.rocksdb.RocksDBEngine;
import org.apache.cassandra.utils.Pair;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import static org.apache.cassandra.rocksdb.RocksDBUtils.getMaxToken;
import static org.apache.cassandra.rocksdb.RocksDBUtils.getMinToken;

public class RocksDBStreamUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStreamWriter.class);
    private static final long INGESTION_WAIT_MS = 100;
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

    public static void ingestRocksSstable(UUID cfId, String sstFile) throws RocksDBException
    {
        ColumnFamilyStore cfs = getColumnFamilyStore(cfId);
        RocksDBCF rocksDBCF = RocksDBEngine.getRocksDBCF(cfId);
        if (cfs == null || rocksDBCF == null)
            return;

        RocksDB db = rocksDBCF.getRocksDB();

        // There might be multiple streaming sessions (threads) for the same sstable/db at the same time.
        // Adding lock to the db to prevent multiple sstables are ingested at same time and trigger
        // write stalls.
        synchronized (db)
        {
            long startTime = System.currentTimeMillis();
            // Wait until compaction catch up by examing the number of l0 sstables.
            while (true)
            {
                int numOfLevel0Sstables = RocksDBUtils.getNumberOfSstablesByLevel(db, 0);
                if (numOfLevel0Sstables <= RocksDBConfigs.LEVEL0_STOP_WRITES_TRIGGER)
                    break;
                try
                {
                    LOGGER.debug("Number of level0 sstables " + numOfLevel0Sstables + " exceeds the threshold " + RocksDBConfigs.LEVEL0_STOP_WRITES_TRIGGER
                                + ", sleep for " + INGESTION_WAIT_MS + "ms.");
                    Thread.sleep(INGESTION_WAIT_MS);
                }
                catch (InterruptedException e)
                {
                    LOGGER.warn("Ingestion wait interrupted, procceding.");
                }
            }
            rocksDBCF.getRocksMetrics().rocksDBIngestWaitTimeHistogram.update(System.currentTimeMillis() - startTime);
            LOGGER.info("Time spend waiting for compaction:" + (System.currentTimeMillis() - startTime));

            long ingestStartTime = System.currentTimeMillis();
            try(final IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions()) {
                db.ingestExternalFile(Arrays.asList(sstFile), ingestExternalFileOptions);
            }

            LOGGER.info("Time spend on ingestion:" + (System.currentTimeMillis() - ingestStartTime));
            rocksDBCF.getRocksMetrics().rocksDBIngestTimeHistogram.update(System.currentTimeMillis() - ingestStartTime);
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

    public static double getRangeSpaceSize(Collection<Range<Token>> normalizedRanges)
    {
        if (normalizedRanges.isEmpty())
            return 0;

        IPartitioner partitioner = normalizedRanges.iterator().next().left.getPartitioner();

        if (partitioner instanceof Murmur3Partitioner)
        {
            double rangesSize = 0;
            for (Range<Token> r : normalizedRanges)
            {
                rangesSize += r.left.size(r.right);
            }
            return rangesSize;
        }

        if (partitioner instanceof RandomPartitioner)
        {
            BigInteger fullTokenSpaceSize = RandomPartitioner.MAXIMUM;
            BigInteger rangesSize = BigInteger.ZERO;
            for (Range<Token> r : normalizedRanges)
            {
                RandomPartitioner.BigIntegerToken left = (RandomPartitioner.BigIntegerToken) r.left;
                RandomPartitioner.BigIntegerToken right = (RandomPartitioner.BigIntegerToken) r.right;
                rangesSize = rangesSize.add(right.getTokenValue()).add(left.getTokenValue().negate());
            }
            return rangesSize.doubleValue() / fullTokenSpaceSize.doubleValue();
        }

        throw new NotImplementedException(partitioner.getClass().getName() + "is not supported");
    }

    public static long estimateDataSize(RocksDBCF rocksDBCF, Collection<Range<Token>> normalizedRange)
    {
        try
        {
            long estimatedDataSize = Long.parseLong(rocksDBCF.getProperty("rocksdb.estimate-live-data-size"));
            return (long)(estimatedDataSize * getRangeSpaceSize(normalizedRange));
        } catch (RocksDBException e)
        {
            LOGGER.warn("Failed to estimate data size", e);
            return 0;
        }
    }

    public static long estimateNumKeys(RocksDBCF rocksDBCF, Collection<Range<Token>> normalizedRange)
    {
        try
        {
            long estimateNumKeys = Long.parseLong(rocksDBCF.getProperty("rocksdb.estimate-num-keys"));
            return (long)(estimateNumKeys * getRangeSpaceSize(normalizedRange));
        } catch (RocksDBException e)
        {
            LOGGER.warn("Failed to estimate num of keys", e);
            return 0;
        }
    }


}
