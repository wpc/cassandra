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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.rocksdb.RocksDBTestBase;

public class RocksDBStreamTestBase extends RocksDBTestBase
{
    public static Path STREAM_DIR;
    public static Path DB_DIR;

    @BeforeClass
    public static void classSetUp() throws Exception
    {
        RocksDBTestBase.classSetUp();

        STREAM_DIR = Paths.get("/tmp/rocksdbstream/", UUID.randomUUID().toString());
        DB_DIR = Paths.get(STREAM_DIR.toAbsolutePath().toString(), "db");
        System.setProperty("cassandra.rocksdb.stream.dir", STREAM_DIR.toString());
        File streamDir = STREAM_DIR.toFile();
        if (streamDir.exists())
        {
            FileUtils.deleteRecursive(streamDir);
        }
        FileUtils.createDirectory(STREAM_DIR.toString());
    }

    @AfterClass
    public static void classTeardown() throws Exception
    {
        RocksDBTestBase.classTeardown();
        System.clearProperty("cassandra.rocksdb.stream.dir");
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
