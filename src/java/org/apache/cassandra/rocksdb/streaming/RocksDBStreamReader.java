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

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.Pair;

public class RocksDBStreamReader
{

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStreamReader.class);
    private final RocksDBMessageHeader header;
    private final StreamSession session;

    public RocksDBStreamReader(RocksDBMessageHeader header, StreamSession session)
    {
        this.header = header;
        this.session = session;
    }

    RocksDBSStableWriter read(DataInputPlus input) throws IOException
    {
        Pair<String, String> kscf = Schema.instance.getCF(header.cfId);
        ColumnFamilyStore cfs = null;
        if (kscf != null)
            cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

        if (kscf == null || cfs == null)
        {
            // schema was dropped during streaming
            throw new IOException("CF " + header.cfId + " was dropped during streaming");
        }

        LOGGER.debug("[Stream #{}] Start receiving rocskdb file #{} from {}, ks = '{}', table = '{}'.",
                     session.planId(), header.sequenceNumber, session.peer, cfs.keyspace.getName(),
                     cfs.getColumnFamilyName());
        RocksDBSStableWriter writer = null;

        try
        {
            writer = new RocksDBSStableWriter(header.cfId);
            while(input.readByte() != RocksDBStreamUtils.EOF[0]) {
                int length = input.readInt();
                byte[] key = new byte[length];
                input.readFully(key);
                length = input.readInt();
                byte[] value = new byte[length];
                input.readFully(value);
                writer.write(key, value);
            }
        }
        catch (Throwable e)
        {
            if (writer != null)
            {
                writer.abort(e);
            }
            throw Throwables.propagate(e);
        }
        writer.close();
        return writer;
    }
}
