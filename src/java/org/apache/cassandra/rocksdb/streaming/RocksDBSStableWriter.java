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

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.rocksdb.RocksDBConfigs;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

public class RocksDBSStableWriter
{

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBSStableWriter.class);
    private final UUID cfId;
    private final int shardId;
    private final EnvOptions envOptions;
    private final Options options;
    private File sstable = null;
    private SstFileWriter sstableWriter = null;
    private long currentSstableSize;
    private volatile long incomingBytes;
    private volatile int sstableIngested;

    public RocksDBSStableWriter(UUID cfId) throws IOException, RocksDBException
    {
        this(cfId, 0);
    }

    public RocksDBSStableWriter(UUID cfId, int shardId) throws IOException, RocksDBException
    {
        this.cfId = cfId;
        this.currentSstableSize = 0;
        this.incomingBytes = 0;
        this.sstableIngested = 0;
        this.envOptions = new EnvOptions();
        this.options = new Options();
        this.shardId = shardId;
        RocksDBThroughputManager.getInstance().registerIncomingStreamWriter(this);
    }

    private synchronized void createSstable() throws IOException, RocksDBException
    {
        sstable = File.createTempFile(cfId.toString(), ".sst", RocksDBConfigs.STREAMING_TMPFILE_PATH);
        sstable.deleteOnExit();
        sstableWriter = new SstFileWriter(envOptions, options);
        sstableWriter.open(sstable.getAbsolutePath());
        LOGGER.info("Created " + sstable + " for receiving streamed data.");
    }

    private synchronized void IngestSstable() throws RocksDBException
    {

        LOGGER.info("Flushing " + sstable + ", estimated size: " + currentSstableSize + ", flushing threshold: " + RocksDBConfigs.SSTABLE_INGEST_THRESHOLD);
        try
        {
            if (sstableWriter != null)
                sstableWriter.finish();
            if (sstable != null && sstable.exists())
            {
                sstableIngested += 1;
                RocksDBStreamUtils.ingestRocksSstable(cfId, shardId, sstable.getAbsolutePath());
            }
        } finally
        {
            if (sstableWriter != null)
                sstableWriter.close();
            sstableWriter = null;
            if (sstable != null)
            {
                sstable.delete();
                sstable = null;
            }
            currentSstableSize = 0;
        }
    }

    public synchronized void write(byte[] key, byte[] value) throws IOException, RocksDBException
    {
        if (sstableWriter == null)
            createSstable();

        sstableWriter.merge(key, value);

        currentSstableSize += key.length + value.length;
        incomingBytes += key.length + value.length + RocksDBStreamUtils.MORE.length + Integer.BYTES * 2;
        if (currentSstableSize >= RocksDBConfigs.SSTABLE_INGEST_THRESHOLD)
            IngestSstable();
    }

    public synchronized void close() throws IOException
    {
        try
        {
            IngestSstable();
        }
        catch (RocksDBException e)
        {
            throw new IOException("rocksdb failed", e);
        }
        finally
        {
            options.close();
            envOptions.close();
        }
    }

    public synchronized Throwable abort(Throwable e)
    {
        try
        {
            close();
        }
        catch (IOException re) {}
        return e;
    }

    public long getIncomingBytes()
    {
        return incomingBytes;
    }

    public int getSstableIngested()
    {
        return sstableIngested;
    }

}
