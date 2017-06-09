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


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBStreamingUtils
{
    public static final byte[] EOF = new byte[]{'\0'};
    public static final byte[] MORE = new byte[]{'1'};

    public static void ingestRocksSstables(RocksDB db, Collection<RocksDBSStableWriter> rocksTables) throws RocksDBException
    {
        final IngestExternalFileOptions ingestExternalFileOptions =
        new IngestExternalFileOptions();
        List<String> files = new ArrayList<>(rocksTables.size());
        for (RocksDBSStableWriter writer : rocksTables)
        {
            files.add(writer.getFile().getAbsolutePath());
        }
        db.ingestExternalFile(files,
                              ingestExternalFileOptions);
    }
}
