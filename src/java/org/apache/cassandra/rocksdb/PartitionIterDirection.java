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

import org.apache.cassandra.rocksdb.encoding.orderly.Bytes;
import org.apache.cassandra.utils.FBUtilities;

enum PartitionIterOrder
{
    NORMAL
    {
        public void moveForward(RocksDBIteratorAdapter rocksIterator)
        {
            rocksIterator.next();
        }

        public void seekToStart(RocksDBIteratorAdapter rocksIterator, byte[] partitionKey, byte[] minKey, byte[] maxKey,
                                boolean lowerExclusive, boolean upperExclusive)
        {
            if (minKey == null)
            {
                rocksIterator.seek(partitionKey);
                return;
            }

            rocksIterator.seek(minKey);
            if (lowerExclusive &&
                rocksIterator.isValid() && FBUtilities.compareUnsigned(rocksIterator.key(), minKey) <= 0)
            {
                rocksIterator.next();
            }
        }
    },

    REVERSED
    {
        public void moveForward(RocksDBIteratorAdapter rocksIterator)
        {
            rocksIterator.prev();
        }

        public void seekToStart(RocksDBIteratorAdapter rocksIterator, byte[] partitionKey, byte[] minKey, byte[] maxKey,
                                boolean lowerExclusive, boolean upperExclusive)
        {
            if (maxKey == null)
            {
                rocksIterator.seek(minKey == null ? partitionKey : minKey);
                moveToPartitionEnd(rocksIterator, partitionKey);
                return;
            }

            rocksIterator.seek(maxKey);

            if (rocksIterator.isValid())
            {
                // we seeked to a valid key that closest to upper bound
                // next we make sure we are on the lower side of upper bound
                int compareResult = FBUtilities.compareUnsigned(rocksIterator.key(), maxKey);
                if ((upperExclusive && compareResult == 0) || compareResult > 0)
                {
                    rocksIterator.prev();
                }
            }
            else
            {
                // we are likely reach the end of the database
                // in this case we just reset to partition end
                rocksIterator.seek(minKey == null ? partitionKey : minKey);
                moveToPartitionEnd(rocksIterator, partitionKey);
            }
        }

        private void moveToPartitionEnd(RocksDBIteratorAdapter rocksIterator, byte[] partitionKey)
        {
            byte[] maxKey = null;
            while (rocksIterator.isValid())
            {
                byte[] key = rocksIterator.key();
                if (!Bytes.startsWith(key, partitionKey))
                {
                    break;
                }
                maxKey = key;
                rocksIterator.next();
            }
            if (maxKey != null)
            {
                rocksIterator.seek(maxKey);
            }
        }
    };

    public abstract void moveForward(RocksDBIteratorAdapter rocksIterator);

    public abstract void seekToStart(RocksDBIteratorAdapter rocksIterator, byte[] partitionKey, byte[] minKey, byte[] maxKey,
                                     boolean lowerExclusive, boolean upperExclusive);

}
