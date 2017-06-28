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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.utils.UUIDGen;

public class BasicSelectTest extends RocksDBTestBase
{

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

        assertRows(execute("SELECT v FROM %s WHERE p=?", "p1"),
                   row("v1"),
                   row("v3"),
                   row("v2"));
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

        assertRows(execute("SELECT c1, c2, v FROM %s WHERE p=?", p1),
                   row(9L, uuid1, ByteBuffer.wrap("v2".getBytes())),
                   row(9L, uuid2, ByteBuffer.wrap("v1".getBytes())),
                   row(9L, uuid3, ByteBuffer.wrap("v3".getBytes())));
    }

    @Test
    public void testSelectWholePartitionRangeQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, f text, PRIMARY KEY (a, b, c, d, e) )");

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 2, '2')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, '1')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 2, 1, '1')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 3, '3')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 5, '5')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (2, 1, 1, 1, 5, '5')");

        assertRows(execute("SELECT a, b, c, d, e, f FROM %s WHERE a = 1"),
                   row(1, 1, 1, 1, 1, "1"),
                   row(1, 1, 1, 1, 2, "2"),
                   row(1, 1, 1, 1, 3, "3"),
                   row(1, 1, 1, 1, 5, "5"),
                   row(1, 1, 1, 2, 1, "1"));
    }

    @Test
    public void testRangeQueryWithMerge() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, f text, g text, PRIMARY KEY (a, b, c, d, e) )");

        execute("INSERT INTO %s (a, b, c, d, e, f, g) VALUES (1, 1, 1, 1, 2, '2', '0')");
        execute("INSERT INTO %s (a, b, c, d, e, f, g) VALUES (1, 1, 1, 1, 1, '1', '0')");
        execute("INSERT INTO %s (a, b, c, d, e, f, g) VALUES (1, 1, 1, 2, 1, '1', '0')");
        execute("INSERT INTO %s (a, b, c, d, e, f, g) VALUES (1, 1, 1, 1, 3, '3', '0')");
        execute("INSERT INTO %s (a, b, c, d, e, f, g) VALUES (1, 1, 1, 1, 5, '5', '0')");
        execute("INSERT INTO %s (a, b, c, d, e, f, g) VALUES (2, 1, 1, 1, 5, '5', '0')");

        execute("DELETE FROM %s WHERE a=1 AND b=1 AND c=1 AND d=1 AND e=1");
        execute("DELETE FROM %s WHERE a=1 AND b=1 AND c=1 AND d=1 AND e=3");
        execute("UPDATE %s SET f='6' WHERE a=1 AND b=1 AND c=1 AND d=1 AND e=5");
        execute("UPDATE %s SET g='1' WHERE a=1 AND b=1 AND c=1 AND d=1 AND e=5");

        assertRows(execute("SELECT a, b, c, d, e, f, g FROM %s WHERE a = 1"),
                   row(1, 1, 1, 1, 2, "2", "0"),
                   row(1, 1, 1, 1, 5, "6", "1"),
                   row(1, 1, 1, 2, 1, "1", "0"));
    }

    @Test
    public void testEmptyResult() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, PRIMARY KEY (p, c))");
        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=?", "p1", "k1"));
    }
}
