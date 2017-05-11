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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.rocksdb.encoding.KeyPart;
import org.apache.cassandra.rocksdb.encoding.orderly.Bytes;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.UUIDGen;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import static org.apache.cassandra.rocksdb.encoding.RowKeyEncoder.encode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicSelectTest extends CQLTester
{
    @BeforeClass
    public static void classSetUp() throws Exception
    {
        System.setProperty("cassandra.rocksdb.keyspace", CQLTester.KEYSPACE);
        System.setProperty("cassandra.rocksdb.dir", "/tmp/rocksdbtest");
        File rocksdbdir = new File("/tmp/rocksdbtest");
        if (rocksdbdir.exists())
        {
            FileUtils.deleteRecursive(rocksdbdir);
        }
    }

    @AfterClass
    public static void classTeardown() throws Exception
    {
        System.clearProperty("cassandra.rocksdb.keyspace");
        System.clearProperty("cassandra.rocksdb.dir");
    }


    @Test
    public void testSelectSingleColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text)");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, firstname) VALUES (?, 'Frodo')", id1);
        execute("INSERT INTO %s (userid, firstname) VALUES (?, 'Samwise')", id2);

        assertRowCount(execute("SELECT firstname FROM %s WHERE userid=?", id1), 1);
    }

    @Test
    public void testSelectSingleColumnWithOneClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, PRIMARY KEY (p, c))");

        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k1", "v1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        assertRows(execute("SELECT p, c, v FROM %s WHERE p=? AND c=?", "p1", "k1"),
                   row("p1", "k1", "v1"));
        assertRows(execute("SELECT p, c, v FROM %s WHERE p=? AND c=?", "p1", "k2"),
                   row("p1", "k2", "v2"));
    }

    @Test
    public void testOrderIsPreservedForSortableClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c bigint, v text, PRIMARY KEY (p, c))");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", 12L, "v3");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", 9L, "v1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", 20L, "v2");

        assertRows(execute("SELECT v FROM %s WHERE p=? and c=?", "p1", 12L),
                   row("v3"));
        assertKeyOrderInRocksDB(encode(new KeyPart("p1", UTF8Type.instance), new KeyPart(9L, LongType.instance)),
                                encode(new KeyPart("p1", UTF8Type.instance), new KeyPart(12L, LongType.instance)),
                                encode(new KeyPart("p1", UTF8Type.instance), new KeyPart(20L, LongType.instance)));
    }

    @Test
    public void testOrderIsPreservedForTimedUUIDKeys() throws Throwable
    {
        long now = System.currentTimeMillis();
        UUID uuid1 = UUIDGen.getTimeUUID(now, 1L);
        UUID uuid2 = UUIDGen.getTimeUUID(now, 2L);
        UUID uuid3 = UUIDGen.getTimeUUID(now, 3L);
        BigInteger p1 = new BigInteger("535959550");

        createTable("CREATE TABLE %s (p varint, c1 bigint, c2 timeuuid, v blob, PRIMARY KEY (p, c1, c2))");

        execute("INSERT INTO %s(p, c1, c2, v) values (?, ?, ?, ?)",
                p1, 9L, uuid2, ByteBuffer.wrap("v1".getBytes()));
        execute("INSERT INTO %s(p, c1, c2, v) values (?, ?, ?, ?)",
                p1, 9L, uuid1, ByteBuffer.wrap("v2".getBytes()));
        execute("INSERT INTO %s(p, c1, c2, v) values (?, ?, ?, ?)",
                p1, 9L, uuid3, ByteBuffer.wrap("v3".getBytes()));

        assertRows(execute("SELECT v FROM %s WHERE p=? and c1=? and c2=?",
                           p1, 9L, uuid3),
                   row(ByteBuffer.wrap("v3".getBytes())));
        KeyPart keyPart1 = new KeyPart(p1, IntegerType.instance);
        KeyPart keyPart2 = new KeyPart(9L, LongType.instance);
        assertKeyOrderInRocksDB(encode(keyPart1, keyPart2, new KeyPart(uuid1, TimeUUIDType.instance)),
                                encode(keyPart1, keyPart2, new KeyPart(uuid2, TimeUUIDType.instance)),
                                encode(keyPart1, keyPart2, new KeyPart(uuid3, TimeUUIDType.instance)));
    }


    private void assertKeyOrderInRocksDB(byte[]... keys) throws IOException
    {
        Iterable<ColumnFamilyStore> cfss = StorageService.instance.getValidColumnFamilies(false, false, KEYSPACE, currentTable());
        ColumnFamilyStore cfs = cfss.iterator().next();
        RocksDB rocksDB = cfs.db;
        ReadOptions readOptions = new ReadOptions();
        readOptions = readOptions.setTotalOrderSeek(true);
        assertEquals(keys.length, iterLength(rocksDB.newIterator()));
        try (RocksIterator rocksIterator = rocksDB.newIterator(readOptions))
        {
            rocksIterator.seekToFirst();
            for (int i = 0; i < keys.length; i++)
            {
                assertTrue(rocksIterator.isValid());
                assertEquals(Bytes.toStringBinary(keys[i]), Bytes.toStringBinary(rocksIterator.key()));
                rocksIterator.next();
            }
        }
    }

    private int iterLength(RocksIterator rocksIterator)
    {
        int i = 0;
        rocksIterator.seekToFirst();
        while (rocksIterator.isValid())
        {
            i++;
            rocksIterator.next();
        }
        return i;
    }
}
