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

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.rocksdb.RocksDBUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RocksDBStreamUtilsTest
{
    private static long[][][] LIST_OF_LONG_RANGES =
    {
      {},
      {
        {Long.MIN_VALUE, Long.MAX_VALUE},
      },
      {
        {Long.MAX_VALUE - 1000, Long.MAX_VALUE},
        {Long.MIN_VALUE, Long.MIN_VALUE + 1000},
      },
      {
        {-4, 0},
        {1, 2},
        {11, 12},
      },
//      {{Long.MAX_VALUE - 1000, Long.MIN_VALUE + 1000}},
//      TODO: Range.normalize can't deal with this case, we probably need fix it.
    };

    private static BigInteger[][][] LIST_OF_BIGINTEGER_RANGES =
    {
        {},
        {
            {RandomPartitioner.ZERO, RandomPartitioner.MAXIMUM},
        },
        {
            {RandomPartitioner.MAXIMUM.add(BigInteger.valueOf(-1000)), RandomPartitioner.MAXIMUM},
            {RandomPartitioner.ZERO, RandomPartitioner.ZERO.add(BigInteger.valueOf(1000))},
        },
        {
            {BigInteger.valueOf(-1), BigInteger.valueOf(2)},
            {BigInteger.valueOf(11), BigInteger.valueOf(15)},
            {BigInteger.valueOf(19), BigInteger.valueOf(100)},
        },
    };

    @Test
    public void testCalculateComplementRanges()
    {
        for (long[][] rawRanges : LIST_OF_LONG_RANGES)
        {
            Collection<Range<Token>> ranges = new ArrayList<>(rawRanges.length);
            for (long[] range : rawRanges)
            {
                ranges.add(new Range<>(new Murmur3Partitioner.LongToken(range[0]),
                                                   new Murmur3Partitioner.LongToken(range[1])));
            }
            Collection<Range<Token>> complementRanges =
                RocksDBStreamUtils.calcluateComplementRanges(Murmur3Partitioner.instance, ranges);
            assertNoOverlap(ranges, complementRanges);
            assertUnionIsFullRing(ranges, complementRanges);
            assertSorted(complementRanges);
        }

        for (BigInteger[][] rawRanges : LIST_OF_BIGINTEGER_RANGES)
        {
            Collection<Range<Token>> ranges = new ArrayList<>(rawRanges.length);
            for (BigInteger[] range : rawRanges)
            {
                ranges.add(new Range<>(new RandomPartitioner.BigIntegerToken(range[0]),
                                       new RandomPartitioner.BigIntegerToken(range[1])));
            }
            Collection<Range<Token>> complementRanges =
            RocksDBStreamUtils.calcluateComplementRanges(RandomPartitioner.instance, ranges);
            assertNoOverlap(ranges, complementRanges);
            assertUnionIsFullRing(ranges, complementRanges);
            assertSorted(complementRanges);
        }
    }

    @Test
    public void testGetRangeSpaceSize()
    {
        assertEquals(0.0, RocksDBStreamUtils.getRangeSpaceSize(new ArrayList<>()), 1e-10);
        assertEquals(1.0,
                     RocksDBStreamUtils.getRangeSpaceSize(
                                                         Arrays.asList(
                                                                        new Range<Token>(Murmur3Partitioner.MINIMUM,
                                                                                         new Murmur3Partitioner.LongToken(Murmur3Partitioner.MAXIMUM
                                                                                         )))),
                     1e-10);

        assertEquals(0.5,
                     RocksDBStreamUtils.getRangeSpaceSize(
                                                         Arrays.asList(
                                                                      new Range<Token>(RandomPartitioner.MINIMUM,
                                                                                       new RandomPartitioner.BigIntegerToken(RandomPartitioner.MAXIMUM.divide(BigInteger.valueOf(2)))
                                                                                       ))),
                     1e-10);
    }

    private void assertNoOverlap(Collection<Range<Token>> ranges, Collection<Range<Token>> complementRanges)
    {
        List<Range<Token>> combined = new ArrayList<>(ranges);
        combined.addAll(complementRanges);
        for (int i = 0; i < combined.size(); i++)
        {
            Range<Token> range0 = combined.get(i);
            for (int j = i + 1; j < combined.size(); j++)
            {
                Range<Token> range1 = combined.get(j);
                assertFalse(range0.intersects(range1));
            }
        }
    }

    private void assertUnionIsFullRing(Collection<Range<Token>> ranges, Collection<Range<Token>> complementRanges)
    {
        List<Range<Token>> combined = new ArrayList<>(ranges);
        combined.addAll(complementRanges);
        Collection<Range<Token>> normalized = Range.normalize(combined);
        assertEquals(normalized.size(), 1);
        Range<Token> range = normalized.iterator().next();
        assertEquals(range.left, RocksDBUtils.getMinToken(range.left.getPartitioner()));
        assertEquals(range.right, RocksDBUtils.getMaxToken(range.left.getPartitioner()));
    }

    private void assertSorted(Collection<Range<Token>> ranges)
    {
        Token previous = null;
        for (Range<Token> range : ranges)
        {
            if (previous != null)
            {
                assertTrue(previous.compareTo(range.left) < 0);
            }
            previous = range.left;
        }
    }
}
