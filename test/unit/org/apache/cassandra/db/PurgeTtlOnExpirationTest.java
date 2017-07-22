/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.BeforeClass;
import org.junit.Test;

public class PurgeTtlOnExpirationTest
{
    private static final String KEYSPACE1 = "PurgeTtlOnExpirationTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    CFMetaData.Builder.create(KEYSPACE1, CF_STANDARD1)
                                                      .addPartitionKey("key", AsciiType.instance)
                                                      .addRegularColumn("col1", AsciiType.instance)
                                                      .build());
    }

    @Test
    public void testPurgeTtlOnExpiration() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        int ttlSeconds = 1;
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.metadata.purgeTtlOnExpiration(Boolean.FALSE);
        String key = "ttl";
        long timestamp = FBUtilities.nowInSeconds();
        ColumnDefinition colDef = cfs.metadata.getColumnDefinition(new ColumnIdentifier("col1", true));
        new RowUpdateBuilder(cfs.metadata, timestamp, ttlSeconds, key)
                    .add("col1", ByteBufferUtil.bytes("val1"))
                    .build()
                    .applyUnsafe();
        cfs.forceBlockingFlush();
        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        Thread.sleep((ttlSeconds + 1) * 1000); // wait for ttl to expire

        // compact with a gcBefore that does *not* allow the row tombstone to be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, (int) (System.currentTimeMillis() / 1000) - 10000, false));

        assertEquals(1, cfs.getLiveSSTables().size());
        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        // confirm ttl expired and turned into tombstone
        assertTrue(partition.iterator().next().getCell(colDef).isTombstone());

        // set purgeTtlOnExpiration=true this time
        cfs.truncateBlocking();
        cfs.metadata.purgeTtlOnExpiration(Boolean.TRUE);
        cfs.reload();
        new RowUpdateBuilder(cfs.metadata, timestamp, ttlSeconds, key)
                    .add("col1", ByteBufferUtil.bytes("val2"))
                    .build()
                    .applyUnsafe();
        cfs.forceBlockingFlush();
        Thread.sleep((ttlSeconds + 1) * 1000); // wait for ttl to expire

        // compact with a gcBefore that does *not* allow the row tombstone to be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, (int) (System.currentTimeMillis() / 1000) - 10000, false));

        assertEquals(1, cfs.getLiveSSTables().size());
        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        // confirm ttl expired and purged, no resulting tombstone
        assertNull(partition.iterator().next().getCell(colDef));
    }

}

