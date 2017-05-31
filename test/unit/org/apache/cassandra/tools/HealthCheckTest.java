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
package org.apache.cassandra.tools;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.utils.HealthCheck;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class HealthCheckTest
{
    @BeforeClass
    public static void setup() throws UnknownHostException
    {
        SchemaLoader.prepareServer();
        InetAddress[] hosts = {
            InetAddress.getByName("127.0.0.1"),
            InetAddress.getByName("127.0.0.2"),
            InetAddress.getByName("127.0.0.5"),
            InetAddress.getByName("127.0.0.9")
        };

        for (InetAddress host : hosts) {
            Gossiper.instance.initializeNodeUnsafe(host, UUID.randomUUID(), 1);
            EndpointState localstate = Gossiper.instance.getEndpointStateForEndpoint(host);
            Gossiper.instance.realMarkAlive(host, localstate);
        }
    }

    @Test
    public void check() throws UnknownHostException
    {
        assertEquals(5, HealthCheck.getStatus());

        SchemaLoader.startGossiper();
        assertEquals(2, HealthCheck.getStatus());

        InetAddress[] deadhosts = {
            InetAddress.getByName("127.0.0.2"),
            InetAddress.getByName("127.0.0.5"),
            InetAddress.getByName("127.0.0.9")
        };

        for (InetAddress host : deadhosts) {
            EndpointState localstate = Gossiper.instance.getEndpointStateForEndpoint(host);
            Gossiper.instance.markDead(host, localstate);
        }
        assertEquals(5, HealthCheck.getStatus());
    }
}
