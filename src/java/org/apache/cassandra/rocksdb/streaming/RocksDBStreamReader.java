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
import java.security.MessageDigest;
import java.util.Arrays;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.rocksdb.RocksDBConfigs;
import org.apache.cassandra.service.DigestMismatchException;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;

public class RocksDBStreamReader
{

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStreamReader.class);
    private static final long INCOMING_BYTES_DELTA_UPDATE_THRESHOLD = 1 * 1024 * 1024;
    private final RocksDBMessageHeader header;
    private final StreamSession session;
    private final MessageDigest digest;
    private final long estimatedIncomingKeys;
    private long totalIncomingBytes;
    private long totalIncomingKeys;

    public RocksDBStreamReader(RocksDBMessageHeader header, StreamSession session)
    {
        this.header = header;
        this.session = session;
        this.totalIncomingBytes = 0;
        this.digest = FBUtilities.newMessageDigest("MD5");
        this.totalIncomingKeys = 0;
        this.estimatedIncomingKeys = header.estimatedKeys;
    }

    public void read(DataInputPlus input) throws IOException
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
        int sstableIngested = 0;
        try
        {
            long incomingBytesDelta = 0;
            for (int expectedShardId = 0; expectedShardId < RocksDBConfigs.NUM_SHARD; expectedShardId ++)
            {
                int shardId = input.readInt();
                LOGGER.info("Receiving shard: " + shardId);
                if (shardId != expectedShardId)
                    throw new StreamingShardMismatchException(header, shardId, expectedShardId);
                writer = new RocksDBSStableWriter(header.cfId, shardId);
                while (input.readByte() != RocksDBStreamUtils.EOF[0])
                {
                    int keyLength = input.readInt();
                    byte[] key = new byte[keyLength];
                    input.readFully(key);
                    int valueLength = input.readInt();
                    byte[] value = new byte[valueLength];
                    input.readFully(value);
                    digest.update(key);
                    digest.update(value);
                    writer.write(key, value);
                    incomingBytesDelta += RocksDBStreamUtils.EOF.length + Integer.BYTES * 2 + keyLength + valueLength;
                    totalIncomingBytes += RocksDBStreamUtils.EOF.length + Integer.BYTES * 2 + keyLength + valueLength;
                    totalIncomingKeys++;
                    if (incomingBytesDelta > INCOMING_BYTES_DELTA_UPDATE_THRESHOLD)
                    {
                        StreamingMetrics.totalIncomingBytes.inc(incomingBytesDelta);
                        incomingBytesDelta = 0;
                        RocksDBStreamUtils.rocksDBProgress(session, header.cfId.toString(), ProgressInfo.Direction.IN, totalIncomingBytes, totalIncomingKeys, estimatedIncomingKeys, false);
                    }
                }
                writer.close();
                sstableIngested += writer.getSstableIngested();
            }
            byte[] actualDigest = digest.digest();
            int expectedDigestLength = input.readInt();
            byte[] expectedDigest = new byte[expectedDigestLength];
            input.readFully(expectedDigest);

            LOGGER.info("Received stream, expected digest: " + Hex.bytesToHex(expectedDigest) + ", actual digest: " + Hex.bytesToHex(actualDigest));
            if (!Arrays.equals(expectedDigest, actualDigest))
            {
                throw new StreamingDigestMismatchException(header, expectedDigest, actualDigest);
            }
            RocksDBStreamUtils.rocksDBProgress(session, header.cfId.toString(), ProgressInfo.Direction.IN, totalIncomingBytes, totalIncomingKeys, estimatedIncomingKeys, true);
        }
        catch (Throwable e)
        {
            if (writer != null)
            {
                writer.abort(e);
            }
            throw Throwables.propagate(e);
        }

        LOGGER.info("[Stream #{}] received {} rocskdb sstables from #{} from {}, ks = '{}', table = '{}'.",
                    session.planId(), sstableIngested, header.sequenceNumber, session.peer, cfs.keyspace.getName(),
                    cfs.getColumnFamilyName());
    }

    public long getTotalIncomingBytes()
    {
        return totalIncomingBytes;
    }

}
