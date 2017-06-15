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
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.Column;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBStreamUtils
{
    public static final byte[] EOF = new byte[]{'\0'};
    public static final byte[] MORE = new byte[]{'1'};

    public static void ingestRocksSstables(RocksDB db, Collection<RocksDBSStableWriter> rocksTables) throws RocksDBException
    {
        final IngestExternalFileOptions ingestExternalFileOptions =
        new IngestExternalFileOptions();
        List<String> files = new ArrayList<>(rocksTables.size());
        for (RocksDBSStableWriter writer : rocksTables)
        {
            files.add(writer.getFile().getAbsolutePath());
        }
        db.ingestExternalFile(files,
                              ingestExternalFileOptions);
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
