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
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.rocksdb.encoding.orderly.RowKey;
import org.apache.cassandra.rocksdb.encoding.orderly.StructRowKey;

public class RowKeyEncoder
{
    public static byte[] encode(ByteBuffer partitionKey, Clustering clustering, CFMetaData metaData)
    {
        List<ColumnDefinition> clusteringColumns = metaData.clusteringColumns();
        List<ColumnDefinition> partitionKeyColumns = metaData.partitionKeyColumns();
        KeyPart[] keyParts = new KeyPart[clusteringColumns.size() + 1];
        keyParts[0] = new KeyPart(partitionKey, partitionKeyColumns.get(0).type);
        for (int i = 1; i < keyParts.length; i++)
        {
            keyParts[i] = new KeyPart(clustering.get(i - 1), clusteringColumns.get(i - 1).type);
        }
        return encode(keyParts);
    }

    public static byte[] encode(KeyPart... keyParts)
    {
        RowKey[] fields = new RowKey[keyParts.length];
        for (int i = 0; i < fields.length; i++)
        {
            fields[i] = keyParts[i].getOrderlyRowKey();
        }

        Object[] values = new Object[keyParts.length];
        for (int i = 0; i < values.length; i++)
        {
            values[i] = keyParts[i].getOrderlyValue();
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
}
