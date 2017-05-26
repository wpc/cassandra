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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
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
import org.apache.cassandra.rocksdb.encoding.orderly.StructRowKey;
import org.apache.cassandra.rocksdb.encoding.orderly.UTF8RowKey;
import org.apache.cassandra.rocksdb.encoding.orderly.VariableLengthByteArrayRowKey;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.rocksdb.encoding.RowKeyInputAdapter.bytesAdapter;
import static org.apache.cassandra.rocksdb.encoding.RowKeyInputAdapter.defalutAdapter;
import static org.apache.cassandra.rocksdb.encoding.RowKeyInputAdapter.timestampAdapter;

public class RowKeyEncoder
{
    private static final Map<AbstractType, RowKeyEncodingPolicy> rowKeyEncodingPolicies = new HashMap<>();

    static
    {
        rowKeyEncodingPolicies.put(AsciiType.instance,
                                   new RowKeyEncodingPolicy(() -> new VariableLengthByteArrayRowKey(), bytesAdapter));
        rowKeyEncodingPolicies.put(BytesType.instance,
                                   new RowKeyEncodingPolicy(() -> new VariableLengthByteArrayRowKey(), bytesAdapter));
        rowKeyEncodingPolicies.put(BooleanType.instance,
                                   new RowKeyEncodingPolicy(() -> new BooleanRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(ByteType.instance,
                                   new RowKeyEncodingPolicy(() -> new ByteRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(DecimalType.instance,
                                   new RowKeyEncodingPolicy(() -> new BigDecimalRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(DoubleType.instance,
                                   new RowKeyEncodingPolicy(() -> new DoubleRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(FloatType.instance,
                                   new RowKeyEncodingPolicy(() -> new FloatRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(InetAddressType.instance,
                                   new RowKeyEncodingPolicy(() -> new VariableLengthByteArrayRowKey(), bytesAdapter));
        rowKeyEncodingPolicies.put(Int32Type.instance,
                                   new RowKeyEncodingPolicy(() -> new IntegerRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(IntegerType.instance,
                                   new RowKeyEncodingPolicy(() -> new BigIntegerRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(LongType.instance,
                                   new RowKeyEncodingPolicy(() -> new LongRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(ShortType.instance,
                                   new RowKeyEncodingPolicy(() -> new ShortRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(SimpleDateType.instance,
                                   new RowKeyEncodingPolicy(() -> new IntegerRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(TimeType.instance,
                                   new RowKeyEncodingPolicy(() -> new LongRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(TimestampType.instance,
                                   new RowKeyEncodingPolicy(() -> new LongRowKey(), timestampAdapter));
        rowKeyEncodingPolicies.put(TimeUUIDType.instance,
                                   new RowKeyEncodingPolicy(() -> new UUIDRowKey(), defalutAdapter));
        rowKeyEncodingPolicies.put(UTF8Type.instance,
                                   new RowKeyEncodingPolicy(() -> new UTF8RowKey(), bytesAdapter));
        rowKeyEncodingPolicies.put(UUIDType.instance,
                                   new RowKeyEncodingPolicy(() -> new UUIDRowKey(), defalutAdapter));
    }

    public static byte[] encode(ByteBuffer partitionKey, Clustering clustering, CFMetaData metaData)
    {
        ColumnDefinition partitionKeyColumn = metaData.partitionKeyColumns().get(0);
        List<ColumnDefinition> clusteringColumns = metaData.clusteringColumns();
        Pair<AbstractType, ByteBuffer>[] keyParts = new Pair[(clusteringColumns.size() + 1)];
        keyParts[0] = Pair.create(partitionKeyColumn.type, partitionKey);
        for (int i = 0; i < clusteringColumns.size(); i++)
        {
            keyParts[i + 1] = Pair.create(clusteringColumns.get(i).type, clustering.get(i));
        }

        return encode(keyParts);
    }


    @SafeVarargs
    public static byte[] encode(Pair<AbstractType, ByteBuffer>... keyParts)
    {
        RowKey[] fields = new RowKey[keyParts.length];
        for (int i = 0; i < fields.length; i++)
        {
            fields[i] = getOrderlyRowKey(keyParts[i]);
        }

        Object[] values = new Object[keyParts.length];
        for (int i = 0; i < values.length; i++)
        {
            values[i] = composeValueForOrderly(keyParts[i]);
        }
        RowKey structRowKey = new StructRowKey(fields);
        try
        {
            return structRowKey.serialize(values);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer[] decode(byte[] key, CFMetaData metadata)
    {
        List<ColumnDefinition> partitionKeyColumns = metadata.partitionKeyColumns();
        List<ColumnDefinition> clusteringColumns = metadata.clusteringColumns();
        List<AbstractType> types = new ArrayList<>(partitionKeyColumns.size() + clusteringColumns.size());
        for (ColumnDefinition partitionKeyColumn : partitionKeyColumns)
        {
            types.add(partitionKeyColumn.type);
        }
        for (ColumnDefinition clusteringColumn : clusteringColumns)
        {
            types.add(clusteringColumn.type);
        }

        return decode(key, types);
    }

    private static ByteBuffer[] decode(byte[] key, List<AbstractType> types)
    {
        RowKey[] fields = new RowKey[types.size()];

        for (int i = 0; i < types.size(); i++)
        {
            fields[i] = getOrderlyRowKey(types.get(i));
        }

        RowKey structRowKey = new StructRowKey(fields);
        Object[] deserialized;
        try
        {
            deserialized = (Object[]) structRowKey.deserialize(key);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        assert deserialized.length == types.size();
        ByteBuffer[] result = new ByteBuffer[deserialized.length];

        for (int i = 0; i < result.length; i++)
        {
            result[i] = decomposeOrderlyValue(deserialized[i], types.get(i));
        }
        return result;
    }

    private static ByteBuffer decomposeOrderlyValue(Object orderlyValue, AbstractType type)
    {
        return rowKeyEncodingPolicies.get(type).decompose(type, orderlyValue);
    }

    private static Object composeValueForOrderly(Pair<AbstractType, ByteBuffer> keyPart)
    {
        return rowKeyEncodingPolicies.get(keyPart.left).compose(keyPart.left, keyPart.right);
    }

    private static RowKey getOrderlyRowKey(Pair<AbstractType, ByteBuffer> keyPart)
    {
        return getOrderlyRowKey(keyPart.left);
    }

    private static RowKey getOrderlyRowKey(AbstractType type)
    {
        return rowKeyEncodingPolicies.get(type).getOrderlyRowKey();
    }
}
