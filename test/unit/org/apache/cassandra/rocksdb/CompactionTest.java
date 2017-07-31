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

import org.junit.Test;

import org.apache.cassandra.db.SinglePartitionReadCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompactionTest extends RocksDBTestBase
{

    @Test
    public void testExpiredColumnsAreConvertedToTombstones() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, v text, PRIMARY KEY (p))");
        execute("INSERT INTO %s(p, v) values (?, ?) USING TIMESTAMP ? AND TTL ?", "p1", "v0",
                System.currentTimeMillis() * 1000 - 140 * 1000000, 80);
        rocksdbFlush();

        execute("INSERT INTO %s(p, v) values (?, ?) USING TIMESTAMP ? AND TTL ?", "p1", "v1",
                System.currentTimeMillis() * 1000 - 120 * 1000000, 80);

        rocksdbFlush();

        // An expired column.
        execute("INSERT INTO %s(p, v) values (?, ?) USING TIMESTAMP ? AND TTL ?", "p1", "v2",
                System.currentTimeMillis() * 1000 - 100 * 1000000, 80);

        rocksdbFlush();

        SinglePartitionReadCommand readCommand = readCommand("p1", "v");
        assertEquals(1, queryEngine(readCommand).size());
        assertTrue(queryEngine(readCommand).get(0).cells().iterator().next().isExpiring());

        triggerCompaction();

        assertEquals(1, queryEngine(readCommand).size());
        assertTrue(queryEngine(readCommand).get(0).cells().iterator().next().isTombstone());
        assertRows(execute("SELECT p, v FROM %s WHERE p=?", "p1"));
    }

    @Test
    public void testExpiredColumnsArePurgedWhenPurgeTtlOptionIsOn() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, v text, PRIMARY KEY (p)) WITH purge_ttl_on_expiration = True");
        execute("INSERT INTO %s(p, v) values (?, ?) USING TIMESTAMP ? AND TTL ?", "p1", "v0",
                System.currentTimeMillis() * 1000 - 140 * 1000000, 80);
        rocksdbFlush();

        execute("INSERT INTO %s(p, v) values (?, ?) USING TIMESTAMP ? AND TTL ?", "p1", "v1",
                System.currentTimeMillis() * 1000 - 120 * 1000000, 80);

        rocksdbFlush();

        // An expired column.
        execute("INSERT INTO %s(p, v) values (?, ?) USING TIMESTAMP ? AND TTL ?", "p1", "v2",
                System.currentTimeMillis() * 1000 - 100 * 1000000, 80);

        rocksdbFlush();

        SinglePartitionReadCommand readCommand = readCommand("p1", "v");
        assertEquals(1, queryEngine(readCommand).size());
        assertTrue(queryEngine(readCommand).get(0).cells().iterator().next().isExpiring());

        triggerCompaction();

        assertEquals(0, queryEngine(readCommand).size());
        assertRows(execute("SELECT p, v FROM %s WHERE p=?", "p1"));
    }
}
