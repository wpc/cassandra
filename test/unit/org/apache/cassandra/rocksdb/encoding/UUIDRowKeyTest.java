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

package org.apache.cassandra.rocksdb.encoding;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.rocksdb.encoding.orderly.Bytes;
import org.apache.cassandra.rocksdb.encoding.orderly.ImmutableBytesWritable;
import org.apache.cassandra.utils.UUIDGen;

import static junit.framework.Assert.assertEquals;

public class UUIDRowKeyTest
{

    public static final UUID TYPE_1 = UUIDGen.getTimeUUID();
    public static final UUID TYPE_2 = new UUID(0xFFFFFFFFFFFF2FFFL, 0xCBFFFFFFFFFFFFFFL);
    public static final UUID TYPE_3 = new UUID(0xEFFFFFFFFFFF3FFFL, 0xCBFFFFFFFFFFFFFFL);
    public static final UUID TYPE_4 = UUID.randomUUID();

    @Test
    public void roundtripSerilizationOfUUIDType() throws IOException
    {
        assertEquals(TYPE_1, roundTrip(TYPE_1));
        assertEquals(TYPE_2, roundTrip(TYPE_2));
        assertEquals(TYPE_3, roundTrip(TYPE_3));
        assertEquals(TYPE_4, roundTrip(TYPE_4));
    }

    @Test
    public void encodedOrderedByVersion() throws IOException
    {
        UUIDRowKey rowKey = new UUIDRowKey();
        byte[] type1 = rowKey.serialize(TYPE_1);
        byte[] type2 = rowKey.serialize(TYPE_2);
        byte[] type3 = rowKey.serialize(TYPE_3);
        byte[] type4 = rowKey.serialize(TYPE_4);
        List<byte[]> encoded = Arrays.asList(type3, type2, type4, type1);
        encoded.sort(new Bytes.ByteArrayComparator());
        assertEquals(Arrays.asList(type1, type2, type3, type4), encoded);
    }

    @Test
    public void encodedOrderedByTimestampForTypeOneUUID() throws IOException
    {
        UUIDRowKey rowKey = new UUIDRowKey();
        byte[] u1 = rowKey.serialize(new UUID(0xffffffff00001000L, 0xbfcb2b3ac88f55f3L));
        byte[] u2 = rowKey.serialize(new UUID(0x1111111100011000L, 0xbfcb2b3ac88f55f3L));
        byte[] u3 = rowKey.serialize(new UUID(0x1111111100001001L, 0xbfcb2b3ac88f55f3L));
        List<byte[]> encoded = Arrays.asList(u3, u1, u2);
        encoded.sort(new Bytes.ByteArrayComparator());
        assertEquals(Arrays.asList(u1, u2, u3), encoded);
    }

    private Object roundTrip(UUID uuid) throws IOException
    {
        UUIDRowKey uuidRowKey = new UUIDRowKey();
        byte[] serialized = uuidRowKey.serialize(uuid);
        return uuidRowKey.deserialize(new ImmutableBytesWritable(serialized));
    }
}