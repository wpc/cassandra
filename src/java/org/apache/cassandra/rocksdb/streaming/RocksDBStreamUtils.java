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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.rocksdb.engine.RocksEngine;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBStreamUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStreamWriter.class);
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

    public static void ingestRocksSstable(UUID cfId, String sstFile) throws RocksDBException
    {
        ColumnFamilyStore cfs = getColumnFamilyStore(cfId);
        RocksDB db = getRocksdb(cfId);
        if (cfs == null || db == null)
            return;
        long startTime = System.currentTimeMillis();
        final IngestExternalFileOptions ingestExternalFileOptions =
        new IngestExternalFileOptions();
        db.ingestExternalFile(Arrays.asList(sstFile),
                              ingestExternalFileOptions);
        ingestExternalFileOptions.close();
        cfs.rocksMetric.rocksdbIngestTimeHistogram.update(System.currentTimeMillis() - startTime);
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
}
