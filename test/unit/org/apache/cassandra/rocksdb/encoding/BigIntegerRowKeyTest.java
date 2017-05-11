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

package org.apache.cassandra.rocksdb.encoding;

import java.io.IOException;
import java.math.BigInteger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BigIntegerRowKeyTest
{

    public static final BigInteger ZERO = new BigInteger("0");
    public static final BigInteger LARG_POSITIVE = new BigInteger("232343243242342342");
    public static final BigInteger LARGE_NEGTIVE = new BigInteger("-232343243242342342");

    @Test
    public void roundtripSerialization() throws Exception
    {
        assertEquals(ZERO, roundtrip(ZERO));
        assertEquals(LARG_POSITIVE, roundtrip(LARG_POSITIVE));
        assertEquals(LARGE_NEGTIVE, roundtrip(LARGE_NEGTIVE));
    }

    private BigInteger roundtrip(BigInteger bi) throws IOException
    {
        BigIntegerRowKey row = new BigIntegerRowKey();
        byte[] serialized = row.serialize(bi);
        return (BigInteger) row.deserialize(serialized);
    }
}