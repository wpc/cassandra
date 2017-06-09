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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RocksDBSStableWriterTest extends RocksDBStreamTestBase
{
    @Test
    public void testWrite() throws IOException, RocksDBException
    {
        RocksDBSStableWriter writer = new RocksDBSStableWriter(UUID.randomUUID());
        for (int i = 0; i < 100; i ++)
        {
            writer.write(serialize(i), serialize(i));
        }
        writer.close();
        assertTrue(writer.getFile().exists());

        try(final Options options = new Options()
                                    .setCreateIfMissing(true)
                                    .setMergeOperatorName("put");
            final RocksDB db = RocksDB.open(options, DB_DIR.toAbsolutePath().toString());
            final IngestExternalFileOptions ingestExternalFileOptions =
            new IngestExternalFileOptions())
        {
            db.ingestExternalFile(Arrays.asList(writer.getFile().getAbsolutePath()),
                                  ingestExternalFileOptions);
            for (int i = 0; i < 100; i ++)
            {
                assertEquals(deserialize(db.get(serialize(i))), i);
            }
            assertEquals(db.get("not existing key".getBytes()), null);
        }
    }

    @Test
    public void testAbort() throws IOException, RocksDBException
    {
        RocksDBSStableWriter writer = new RocksDBSStableWriter(UUID.randomUUID());
        for (int i = 0; i < 100; i ++)
        {
            writer.write(serialize(i), serialize(i));
        }
        writer.abort(null);
        assertFalse(writer.getFile().exists());
    }

    private byte[] serialize(int value)
    {
        return ByteBufferUtil.getArray(Int32Serializer.instance.serialize(value));
    }

    private int deserialize(byte[] value)
    {
        return Int32Serializer.instance.deserialize(ByteBuffer.wrap(value));
    }
}
