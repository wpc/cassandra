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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

public class RocksDBMessageHeader
{
    public static RocksdbMessageHeaderSerializer SERIALIZER = new RocksdbMessageHeaderSerializer();
    public final UUID cfId;
    public final int sequenceNumber;
    public final long estimatedBytes;
    public final long estimatedKeys;

    public RocksDBMessageHeader(UUID cfId, int sequenceNumber) {
        this(cfId, sequenceNumber, 0, 0);
    }

    public RocksDBMessageHeader(UUID cfId, int sequenceNumber, long estimatedBytes, long estimatedKeys) {
        this.cfId = cfId;
        this.sequenceNumber = sequenceNumber;
        this.estimatedBytes = estimatedBytes;
        this.estimatedKeys = estimatedKeys;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("RocksHeader (");
        sb.append("cfId: ").append(cfId);
        sb.append(", #").append(sequenceNumber);
        sb.append(", estimated number of bytes").append(sequenceNumber);
        sb.append(", estimated number of keys").append(sequenceNumber);
        sb.append(')');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RocksDBMessageHeader that = (RocksDBMessageHeader) o;
        return sequenceNumber == that.sequenceNumber && cfId.equals(that.cfId);
    }

    @Override
    public int hashCode()
    {
        int result = cfId.hashCode();
        result = 31 * result + sequenceNumber;
        return result;
    }

    static class RocksdbMessageHeaderSerializer
    {
        public void seriliaze(RocksDBMessageHeader header, DataOutputPlus out) throws IOException
        {
            org.apache.cassandra.utils.UUIDSerializer.serializer.serialize(header.cfId, out, MessagingService.current_version);
            out.writeInt(header.sequenceNumber);
            out.writeLong(header.estimatedBytes);
            out.writeLong(header.estimatedKeys);
        }

        public RocksDBMessageHeader deserialize(DataInputPlus in) throws IOException
        {
            UUID cfId = org.apache.cassandra.utils.UUIDSerializer.serializer.deserialize(in, MessagingService.current_version);
            int sequenceNumber = in.readInt();
            long estimatedBytes = in.readLong();
            long estimatedKeys = in.readLong();
            return new RocksDBMessageHeader(cfId, sequenceNumber, estimatedBytes, estimatedKeys);
        }

    }

}
