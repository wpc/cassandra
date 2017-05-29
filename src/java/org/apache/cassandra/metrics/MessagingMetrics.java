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
package org.apache.cassandra.metrics;

import com.codahale.metrics.Counter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics related to messages.
 */
public class MessagingMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Messaging");

    private static final ConcurrentHashMap<Pair<MessagingService.Verb, String>, Counter> crossRegionCounter = new ConcurrentHashMap<>();

    public static void addCrossRegionCounter(MessagingService.Verb verb, InetAddress remote)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(remote);

        Pair<MessagingService.Verb, String> mapKey = Pair.create(verb, remoteDC);
        Counter counter = crossRegionCounter.get(mapKey);
        if (counter == null)
        {
            String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
            final String metricKey = verb + "." + localDC + "." + remoteDC;
            counter = crossRegionCounter.computeIfAbsent(mapKey, k -> Metrics.counter(factory.createMetricName(metricKey)));
        }
        counter.inc();
    }
}
