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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.rocksdb.encoding.RowKeyEncoder;
import org.apache.cassandra.rocksdb.encoding.value.ColumnEncoder;
import org.apache.cassandra.rocksdb.encoding.value.RowValueEncoder;
import org.apache.cassandra.utils.Hex;
import org.rocksdb.RocksDBException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class RocksDBCFTest extends RocksDBTestBase
{
    @Test
    public void testMerge() throws RocksDBException
    {
        createTable("CREATE TABLE %s (p text, c text, v text, PRIMARY KEY (p, c))");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        
        RocksDBCF rocksDBCF = RocksDBEngine.getRocksDBCF(cfs.metadata.cfId);
        byte[] key = "test_key".getBytes();
        byte[] value = encodeValue(cfs, "test_value");
        rocksDBCF.merge(key, value);
        assertArrayEquals(value, rocksDBCF.get(key));
    }

    private byte[] encodeValue(ColumnFamilyStore cfs, String value)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        ColumnDefinition v = cfs.metadata.getColumnDefinition(ColumnIdentifier.getInterned("v", true));
        BufferCell cell = BufferCell.live(cfs.metadata, v, 0, ByteBuffer.wrap(value.getBytes()));
        builder.addCell(cell);
        return RowValueEncoder.encode(cfs.metadata, builder.build());
    }

    @Test
    public void testTruncate() throws RocksDBException
    {
        createTable("CREATE TABLE %s (p text, c text, v text, PRIMARY KEY (p, c))");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        RocksDBCF rocksDBCF = RocksDBEngine.getRocksDBCF(cfs.metadata.cfId);

        byte[] key = "test_key".getBytes();
        byte[] value = encodeValue(cfs, "test_value");

        rocksDBCF.merge(key, value);
        assertArrayEquals(value, rocksDBCF.get(key));

        rocksDBCF.truncate();

        rocksDBCF.compactRange();
        assertNull(rocksDBCF.get(key));
    }

    @Test
    public void testClose() throws RocksDBException
    {
        createTable("CREATE TABLE %s (p text, c text, v text, PRIMARY KEY (p, c))");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        RocksDBCF rocksDBCF = RocksDBEngine.getRocksDBCF(cfs.metadata.cfId);

        byte[] key = "test_key".getBytes();
        byte[] value = encodeValue(cfs, "test_value");

        rocksDBCF.merge(key, value);

        assertArrayEquals(value, rocksDBCF.get(key));

        cfs.engine.close(cfs);
    }

    @Test
    public void testDumpPrefix() throws Exception
    {
        createTable("CREATE TABLE %s (p text, c text, v text, PRIMARY KEY (p, c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        RocksDBCF rocksDBCF = RocksDBEngine.getRocksDBCF(cfs.metadata.cfId);

        rocksDBCF.merge("test_key1".getBytes(), "test_value11".getBytes());
        rocksDBCF.merge("test_key1".getBytes(), "test_value12".getBytes());
        rocksDBCF.merge("test_key2".getBytes(), "test_value2".getBytes());

        String dump = rocksDBCF.dumpPrefix("test_key".getBytes(), Integer.MAX_VALUE);
        assertEquals(2, dump.split("\n").length);
        String dumpLimited = rocksDBCF.dumpPrefix("test_key".getBytes(), 1);
        assertEquals(1, dumpLimited.split("\n").length);
    }
}
