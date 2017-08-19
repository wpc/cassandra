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


import org.apache.cassandra.metrics.RocksdbTableMetrics;
import org.rocksdb.RocksIterator;

public class RocksIteratorAdapter
{
    private final RocksIterator iterator;
    private final RocksdbTableMetrics metrics;

    RocksIteratorAdapter(RocksIterator rocksIterator, RocksdbTableMetrics metrics)
    {
        this.metrics = metrics;
        this.iterator = rocksIterator;
    }

    public void next()
    {
        metrics.rocksdbIterMove.inc();
        iterator.next();
    }

    public void seek(byte[] key)
    {
        metrics.rocksdbIterSeek.inc();
        iterator.seek(key);
    }

    public boolean isValid()
    {
        return iterator.isValid();
    }

    public byte[] key()
    {
        return iterator.key();
    }

    public void prev()
    {
        metrics.rocksdbIterMove.inc();
        iterator.prev();
    }

    public void close()
    {
        iterator.close();
    }

    public byte[] value()
    {
        return iterator.value();
    }
}
