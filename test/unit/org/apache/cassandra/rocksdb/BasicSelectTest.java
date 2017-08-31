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

    @Test
    public void testRangeQueryWithDescClusteringKey() throws Throwable
    {
        long now = System.currentTimeMillis();
        UUID uuid1 = UUIDGen.getTimeUUID(now, 1L);
        UUID uuid2 = UUIDGen.getTimeUUID(now, 2L);
        UUID uuid3 = UUIDGen.getTimeUUID(now, 3L);
        BigInteger p1 = new BigInteger("535959550");

        createTable("CREATE TABLE %s (" +
                    "key varint, " +
                    "column1 timeuuid, " +
                    "value text, " +
                    "PRIMARY KEY (key, column1))" +
                    "WITH CLUSTERING ORDER BY (column1 DESC)");

        execute("INSERT INTO %s(key, column1, value) values (?, ?, ?)",
                p1, uuid2, "v1");
        execute("INSERT INTO %s(key, column1, value) values (?, ?, ?)",
                p1, uuid1, "v2");
        execute("INSERT INTO %s(key, column1, value) values (?, ?, ?)",
                p1, uuid3, "v3");

        assertRows(execute("SELECT * FROM %s WHERE key=?", p1),
                   row(p1, uuid3, "v3"),
                   row(p1, uuid2, "v1"),
                   row(p1, uuid1, "v2"));
    }

    @Test
    public void testRangeQueryWithMixedOrderClusteringKey() throws Throwable
    {
        long now = System.currentTimeMillis();
        UUID uuid1 = UUIDGen.getTimeUUID(now, 1L);
        UUID uuid2 = UUIDGen.getTimeUUID(now, 2L);
        UUID uuid3 = UUIDGen.getTimeUUID(now, 3L);
        UUID uuid4 = UUIDGen.getTimeUUID(now, 4L);
        BigInteger p1 = new BigInteger("535959550");
        BigInteger big0 = BigInteger.valueOf(0);
        BigInteger big1 = BigInteger.valueOf(1);

        createTable("CREATE TABLE %s (" +
                    "key varint, " +
                    "column1 varint, " +
                    "column2 timeuuid, " +
                    "value text, " +
                    "PRIMARY KEY (key, column1, column2))" +
                    "WITH CLUSTERING ORDER BY (column1 ASC, column2 DESC)");


        execute("INSERT INTO %s(key, column1, column2, value) values (?, ?, ?, ?)",
                p1, big0, uuid2, "v1");
        execute("INSERT INTO %s(key, column1, column2, value) values (?, ?, ?, ?)",
                p1, big0, uuid1, "v2");
        execute("INSERT INTO %s(key, column1, column2, value) values (?, ?, ?, ?)",
                p1, big0, uuid3, "v3");
        execute("INSERT INTO %s(key, column1, column2, value) values (?, ?, ?, ?)",
                p1, big1, uuid4, "v4");


        assertRows(execute("SELECT * FROM %s WHERE key=?", p1),
                   row(p1, big0, uuid3, "v3"),
                   row(p1, big0, uuid2, "v1"),
                   row(p1, big0, uuid1, "v2"),
                   row(p1, big1, uuid4, "v4"));
    }

    @Test
    public void testSingleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text, PRIMARY KEY (p, c))");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k2", "v2", "sv1");
        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p2", "k3", "v3", "sv2");

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );


        // Ascending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p1"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p2"),
                   row("p2", "k3", "sv2", "v3")
        );

        // Descending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p1"),
                   row("p1", "k2", "sv1", "v2"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p2"),
                   row("p2", "k3", "sv2", "v3")
        );

        // No order with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k2"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =?", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k0"));

        // Ascending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k2"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =? ORDER BY c ASC", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k0"));

        // Descending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k1"),
                   row("p1", "k2", "sv1", "v2"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k2"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =? ORDER BY c DESC", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k0"));

        // IN

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?)", "p1", "k1", "k2"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c ASC", "p1", "k1", "k2"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c DESC", "p1", "k1", "k2"),
                   row("p1", "k2", "sv1", "v2"),
                   row("p1", "k1", "sv1", "v1")
        );
    }

    @Test
    public void testSingleClusteringReversed() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k2", "v2", "sv1");
        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p2", "k3", "v3", "sv2");

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
                   row("p1", "k2", "sv1", "v2"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p2"),
                   row("p2", "k3", "sv2", "v3")
        );

        // Ascending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p1"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p2"),
                   row("p2", "k3", "sv2", "v3")
        );

        // Descending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p1"),
                   row("p1", "k2", "sv1", "v2"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p2"),
                   row("p2", "k3", "sv2", "v3")
        );

        // No order with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k1"),
                   row("p1", "k2", "sv1", "v2"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k2"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=?", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k0"));

        // Ascending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k2"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=? ORDER BY c ASC", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k0"));

        // Descending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k1"),
                   row("p1", "k2", "sv1", "v2"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k2"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=? ORDER BY c DESC", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k1"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k0"));

        // IN

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?)", "p1", "k1", "k2"),
                   row("p1", "k2", "sv1", "v2"),
                   row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c ASC", "p1", "k1", "k2"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c DESC", "p1", "k1", "k2"),
                   row("p1", "k2", "sv1", "v2"),
                   row("p1", "k1", "sv1", "v1")
        );
    }

    @Test
    public void testSelectBounds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);

        assertRowCount(execute("SELECT v FROM %s WHERE k = 0"), 10);

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c <= 6"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c <= 6"),
                   row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c < 6"),
                   row(2), row(3), row(4), row(5));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c < 6"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c <= 6 LIMIT 2"),
                   row(3), row(4));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c < 6 ORDER BY c DESC LIMIT 2"),
                   row(5), row(4));

        assertEmpty(execute("SELECT v FROM %s WHERE k = 0 AND c > 10"));
    }

    @Test
    public void testSelectBoundsNotExistsInDB() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        for (int i = 0; i < 10; i += 3) // 0, 3, 6, 9
                execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);

        assertRowCount(execute("SELECT v FROM %s WHERE k = 0"), 4);

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 1 AND c <= 7"),
                   row(3), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 1 AND c <= 7"),
                   row(3), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 1 AND c < 7"),
                   row(3), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 1 AND c < 7"),
                   row(3), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 1 AND c <= 7 LIMIT 2"),
                   row(3), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 1 AND c < 7 ORDER BY c DESC LIMIT 2"),
                   row(6), row(3));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 1 AND c <= 7 ORDER BY c DESC LIMIT 2"),
                   row(6), row(3));


        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > -1 LIMIT 2"),
                   row(0), row(3));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c < 10 LIMIT 2"),
                   row(0), row(3));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > -1 ORDER BY C DESC LIMIT 2"),
                   row(9), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c < 10 ORDER BY C DESC LIMIT 2"),
                   row(9), row(6));

    }

    @Test
    public void testCompositeRowKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 varint, k2 int, c int, v int, PRIMARY KEY ((k1, k2), c))");

        BigInteger zero = BigInteger.valueOf(0);
        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (k1, k2, c, v) VALUES (?, ?, ?, ?)", zero, i, i, i);

        assertRows(execute("SELECT * FROM %s WHERE k1 = ? and k2 = 2", zero),
                   row(zero, 2, 2, 2));


        assertRows(execute("SELECT * FROM %s WHERE k1 = ? and k2 IN (1, 3)", zero),
                   row(zero, 1, 1, 1),
                   row(zero, 3, 3, 3));

        assertInvalid("SELECT * FROM %s WHERE k2 = 3");
    }
}
