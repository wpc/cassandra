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

import org.apache.commons.lang.NotImplementedException;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBUtils
{
    public static int getNumberOfSstablesByLevel(RocksDB db, int level) throws RocksDBException
    {
        return Integer.parseInt(db.getProperty("rocksdb.num-files-at-level" + level));
    }

    public static long getPendingCompactionBytes(RocksDB db) throws RocksDBException
    {
        return Long.parseLong(db.getProperty("rocksdb.estimate-pending-compaction-bytes"));
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
