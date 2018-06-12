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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.rocksdb.encoding.orderly.RowKey;

class RowKeyEncodingPolicy
{
    public final RowKeyFactory rowKeyFactory;
    public final RowKeyInputAdapter inputAdapter;
    private Integer encodedLength;

    public RowKeyEncodingPolicy(RowKeyFactory rowKeyFactory, RowKeyInputAdapter inputAdapter)
    {
        this(rowKeyFactory, inputAdapter, null);
    }


    public RowKeyEncodingPolicy(RowKeyFactory rowKeyFactory, RowKeyInputAdapter inputAdapter, Integer encodedLength)
    {
        this.rowKeyFactory = rowKeyFactory;
        this.inputAdapter = inputAdapter;
        this.encodedLength = encodedLength;
    }


    public RowKey getOrderlyRowKey()
    {
        return rowKeyFactory.create();
    }

    public ByteBuffer decompose(AbstractType type, Object orderlyValue)
    {
        return inputAdapter.decompose(type, orderlyValue);
    }

    public Object compose(AbstractType<?> type, ByteBuffer value)
    {
        return inputAdapter.compose(type, value);
    }

    public Integer getEncodedLength()
    {
        return encodedLength;
    }
}
