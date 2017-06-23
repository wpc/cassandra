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
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.rocksdb.encoding.RowKeyEncoder;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.FBUtilities;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

public class RocksDBStreamWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStreamWriter.class);
    private final RocksDB db;
    private final Collection<Range<Token>> ranges;
    private final StreamSession session;

    public RocksDBStreamWriter(RocksDB db, Collection<Range<Token>> ranges, StreamSession session)
    {
        this.db = db;
        this.ranges = RocksDBStreamUtils.normalizeRanges(ranges);
        this.session = session;
    }

    public void write(DataOutputStreamPlus out) throws IOException
    {
        int streamedPairs = 0;
        // Iterate through all possible key-value pairs and send to stream.
        for (Range<Token> range : ranges) {
            RocksIterator iterator = db.newIterator();
            try
            {
                iterator.seekToFirst();
                iterator.seek(RowKeyEncoder.encodeToken(range.left));
                byte[] stop = RowKeyEncoder.encodeToken(range.right);
                while (iterator.isValid())
                {
                    byte[] key = iterator.key();
                    if (FBUtilities.compareUnsigned(key, stop) >= 0)
                        break;
                    out.write(RocksDBStreamUtils.MORE);
                    out.writeInt(key.length);
                    out.write(key);
                    byte[] value = iterator.value();
                    out.writeInt(value.length);
                    out.write(value);
                    streamedPairs++;
                    iterator.next();
                }
            } finally
            {
                iterator.close();
            }
        }
        LOGGER.info("Ranges streamed: " + ranges);
        LOGGER.info("Number of rocksdb entries written: " + streamedPairs);
        out.write(RocksDBStreamUtils.EOF);
    }
}
