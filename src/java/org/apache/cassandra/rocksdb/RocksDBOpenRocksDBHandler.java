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

package org.apache.cassandra.rocksdb;

import org.rocksdb.CassandraCompactionFilter;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

/*
 * Holds data passed from openRocksDB to openAllShards
 */
public class RocksDBOpenRocksDBHandler
{
    private final RocksDB rocksDB;
    private final ColumnFamilyHandle metaCfHandle;
    private final ColumnFamilyHandle dataCfHandle;
    private final ColumnFamilyHandle indexCfHandle;
    private final CassandraCompactionFilter compactionFilter;

    public RocksDBOpenRocksDBHandler(RocksDB rocksDB,
                                     ColumnFamilyHandle metaCfHandle,
                                     ColumnFamilyHandle dataCfHandle,
                                     ColumnFamilyHandle indexCfHandle,
                                     CassandraCompactionFilter compactionFilter)
    {
        this.rocksDB = rocksDB;
        this.metaCfHandle = metaCfHandle;
        this.dataCfHandle = dataCfHandle;
        this.indexCfHandle = indexCfHandle;
        this.compactionFilter = compactionFilter;
    }

    public RocksDB getRocksDB()
    {
        return rocksDB;
    }

    public ColumnFamilyHandle getMetaCfHandle()
    {
        return metaCfHandle;
    }

    public ColumnFamilyHandle getDataCfHandle()
    {
        return dataCfHandle;
    }

    public ColumnFamilyHandle getIndexCfHandle() { return indexCfHandle; }

    public CassandraCompactionFilter getCompactionFilter()
    {
        return compactionFilter;
    }
}
