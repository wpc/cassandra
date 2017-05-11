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


import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.rocksdb.encoding.orderly.BigDecimalRowKey;
import org.apache.cassandra.rocksdb.encoding.orderly.DoubleRowKey;
import org.apache.cassandra.rocksdb.encoding.orderly.FloatRowKey;
import org.apache.cassandra.rocksdb.encoding.orderly.IntegerRowKey;
import org.apache.cassandra.rocksdb.encoding.orderly.LongRowKey;
import org.apache.cassandra.rocksdb.encoding.orderly.RowKey;
import org.apache.cassandra.rocksdb.encoding.orderly.UTF8RowKey;
import org.apache.cassandra.rocksdb.encoding.orderly.VariableLengthByteArrayRowKey;

public class KeyPart
{
    private static final Map<AbstractType, RowKeyFactory> rowKeyFactories = new HashMap<>();

    static
    {
        rowKeyFactories.put(AsciiType.instance, VariableLengthByteArrayRowKey::new);
        rowKeyFactories.put(BytesType.instance, VariableLengthByteArrayRowKey::new);
        rowKeyFactories.put(BooleanType.instance, BooleanRowKey::new);
        rowKeyFactories.put(ByteType.instance, ByteRowKey::new);
        rowKeyFactories.put(DecimalType.instance, BigDecimalRowKey::new);
        rowKeyFactories.put(DoubleType.instance, DoubleRowKey::new);
        rowKeyFactories.put(FloatType.instance, FloatRowKey::new);
        rowKeyFactories.put(InetAddressType.instance, VariableLengthByteArrayRowKey::new);
        rowKeyFactories.put(Int32Type.instance, IntegerRowKey::new);
        rowKeyFactories.put(IntegerType.instance, BigIntegerRowKey::new);
        rowKeyFactories.put(LongType.instance, LongRowKey::new);
        rowKeyFactories.put(ShortType.instance, ShortRowKey::new);
        rowKeyFactories.put(SimpleDateType.instance, IntegerRowKey::new);
        rowKeyFactories.put(TimeType.instance, LongRowKey::new);
        rowKeyFactories.put(TimestampType.instance, LongRowKey::new);
        rowKeyFactories.put(TimeUUIDType.instance, UUIDRowKey::new);
        rowKeyFactories.put(UTF8Type.instance, UTF8RowKey::new);
        rowKeyFactories.put(UUIDType.instance, UUIDRowKey::new);
    }

    private static final Map<AbstractType, RowKeyInputAdapter> rowKeyAdapters = new HashMap<>();

    static
    {
        rowKeyAdapters.put(AsciiType.instance, ByteBuffer::array);
        rowKeyAdapters.put(BytesType.instance, ByteBuffer::array);
        rowKeyAdapters.put(InetAddressType.instance, ByteBuffer::array);
        rowKeyAdapters.put(TimestampType.instance, input -> TimestampType.instance.compose(input).getTime());
        rowKeyAdapters.put(UTF8Type.instance, ByteBuffer::array);
    }

    private final ByteBuffer value;
    private final AbstractType<?> type;

    KeyPart(ByteBuffer value, AbstractType<?> type)
    {
        if (!rowKeyFactories.containsKey(type))
        {
            throw new RuntimeException("Type " + type.toString() + " is not supported by rocksdb engine yet");
        }
        this.value = value;
        this.type = type;
    }

    // for unit test only
    public KeyPart(Object obj, AbstractType type)
    {
        this(type.getSerializer().serialize(obj), type);
    }

    RowKey getOrderlyRowKey()
    {
        return rowKeyFactories.get(type).create();
    }

    Object getOrderlyValue()
    {
        if (rowKeyAdapters.containsKey(type))
        {
            return rowKeyAdapters.get(type).convert(value);
        }
        return type.compose(value);
    }
}
