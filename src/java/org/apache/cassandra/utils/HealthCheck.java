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
import org.apache.cassandra.gms.Gossiper;

public class HealthCheck
{
    private static final double THRESHOLD = 0.5;

    HealthCheck()
    {
    }

    @VisibleForTesting
    public static int getStatus()
    {
        Status status;
        // Gossip is Running
        if ( Gossiper.instance.isEnabled())
        {
            int liveNodes = Gossiper.instance.getLiveMembers().size();
            int unreachableNodes = Gossiper.instance.getUnreachableMembers().size();

            // Check condition for nodes in the cluster. Need to be discussed.
            if (liveNodes/(double)(liveNodes + unreachableNodes) < THRESHOLD)
                status = Status.WARNING;
            else
                status = Status.ALIVE;
        }
        else
            status = Status.WARNING;

        return status.getValue();
    }

    // Copy from com.facebook.swift.fb303.FbStatus
    enum Status
    {
        DEAD(0), STARTING(1), ALIVE(2), STOPPING(3), STOPPED(4), WARNING(5);

        private final int value;

        Status(int value)
        {
            this.value = value;
        }

        public int getValue()
        {
            return value;
        }
    }
}
