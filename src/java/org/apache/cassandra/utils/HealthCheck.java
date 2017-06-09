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
package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

public class HealthCheck
{
    HealthCheck()
    {
    }

    @VisibleForTesting
    public static int getNumLiveDataNodes()
    {
        int liveNodes = 0;
        if ( Gossiper.instance.isEnabled())
        {
            Set<Map.Entry<InetAddress, EndpointState>> EndpointStates = Gossiper.instance.getEndpointStates();

            for (Map.Entry<InetAddress, EndpointState> item : EndpointStates) {
                EndpointState endpointState = item.getValue();
                VersionedValue tokens = endpointState.getApplicationState(ApplicationState.TOKENS);
                if (tokens != null && endpointState.isAlive())
                    liveNodes++;
            }
        }

        return liveNodes;
    }

    public static Set<String> getKeyspaces()
    {
        return Schema.instance.getKeyspaces();
    }
}
