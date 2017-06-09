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

import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.SstFileWriter;

public class RocksDBSStableWriter
{
    private static final File TMP_STREAM_PATH = new File(System.getProperty("cassandra.rocksdb.stream.dir", "/data/rocksdbstream/"));
    private final UUID cfId;
    private final EnvOptions envOptions;
    private final Options options;
    private final File sstable;
    private final SstFileWriter sstableWriter;

    public File getFile()
    {
        return sstable;
    }

    public RocksDBSStableWriter(UUID cfId) throws IOException, RocksDBException
    {
        this.cfId = cfId;
        sstable = File.createTempFile(cfId.toString(), ".sst", TMP_STREAM_PATH);
        sstable.deleteOnExit();
        envOptions = new EnvOptions();
        options = new Options();
        sstableWriter = new SstFileWriter(envOptions, options);
        sstableWriter.open(sstable.getAbsolutePath());
    }

    public String getFilename()
    {
        return sstable.getAbsolutePath();
    }

    public UUID getCfId()
    {
        return cfId;
    }

    public void write(byte[] key, byte[] value) throws IOException, RocksDBException
    {
        Slice keySlice = new Slice(key);
        Slice valueSlice = new Slice(value);
        sstableWriter.merge(keySlice, valueSlice);
        keySlice.close();
        valueSlice.close();
    }

    public void close() throws IOException
    {
        try
        {
            sstableWriter.finish();
        }
        catch (RocksDBException e)
        {
            throw new IOException("rocksdb failed", e);
        }
        finally
        {
            sstableWriter.close();
            options.close();
            envOptions.close();
        }
    }

    public Throwable abort(Throwable e)
    {
        try
        {
            close();
        }
        catch (IOException re) {}
        finally
        {
            sstable.delete();
        }
        return e;
    }
}
