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

import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.SstFileWriter;

public class RocksDBSStableWriter
{

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBSStableWriter.class);
    private static final File TMP_STREAM_PATH = new File(System.getProperty("cassandra.rocksdb.stream.dir", "/data/rocksdbstream/"));
    // While streaming, split the stream into 64 MB sst files.
    private static long SSTABLE_INGEST_THRESHOLD = Long.parseLong(System.getProperty("cassandra.rocksdb.stream.sst_size", "67108864"));
    private final UUID cfId;
    private final EnvOptions envOptions;
    private final Options options;
    private File sstable = null;
    private SstFileWriter sstableWriter = null;
    private long currentSstableSize;
    private volatile int sstableIngested;

    public RocksDBSStableWriter(UUID cfId) throws IOException, RocksDBException
    {
        this.cfId = cfId;
        this.currentSstableSize = 0;
        this.sstableIngested = 0;
        this.envOptions = new EnvOptions();
        this.options = new Options();
    }

    private synchronized void createSstable() throws IOException, RocksDBException
    {
        sstable = File.createTempFile(cfId.toString(), ".sst", TMP_STREAM_PATH);
        sstable.deleteOnExit();
        sstableWriter = new SstFileWriter(envOptions, options);
        sstableWriter.open(sstable.getAbsolutePath());
        LOGGER.info("Created " + sstable + " for receiving streamed data.");
    }

    private synchronized void IngestSstable() throws RocksDBException
    {

        LOGGER.info("Flushing " + sstable + ", estimated size: " + currentSstableSize + ", flushing threshold: " + SSTABLE_INGEST_THRESHOLD);
        try
        {
            if (sstableWriter != null)
                sstableWriter.finish();
            if (sstable != null && sstable.exists())
            {
                sstableIngested += 1;
                RocksDBStreamUtils.ingestRocksSstable(cfId, sstable.getAbsolutePath());
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

        Slice keySlice = new Slice(key);
        Slice valueSlice = new Slice(value);
        sstableWriter.merge(keySlice, valueSlice);
        keySlice.close();
        valueSlice.close();

        currentSstableSize += key.length + value.length;
        if (currentSstableSize >= SSTABLE_INGEST_THRESHOLD)
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

    public int getSstableIngested()
    {
        return sstableIngested;
    }

}
