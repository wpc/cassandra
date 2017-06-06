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

package org.apache.cassandra.rocksdb.engine;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public interface StorageEngine
{
    /*
    public abstract Keyspace openKeyspace(String keyspaceName);
    public abstract Keyspace createKeyspace(String keyspaceName);
    public abstract void dropKeyspace(String keyspaceName);
    public abstract void clearKeyspace(String keyspaceName);

    public abstract ColumnFamilyStore getColumnFamilyStore(String cfName);
    public abstract Collection<ColumnFamilyStore> getColumnFamilyStores();
    public abstract ColumnFamilyStore createColumnFamilyStore(String columnFamily,
                                                              CFMetaData metadata);

    public abstract List<Future<?>> flush();
    */

    void openColumnFamilyStore(String keyspaceName,
                               String columnFamilyName,
                               CFMetaData metadata);

    void apply(final ColumnFamilyStore cfs,
               final PartitionUpdate partitionUpdate,
               final boolean writeCommitLog);

    UnfilteredRowIterator queryStorage(ColumnFamilyStore cfs,
                                       SinglePartitionReadCommand readCommand);
}
