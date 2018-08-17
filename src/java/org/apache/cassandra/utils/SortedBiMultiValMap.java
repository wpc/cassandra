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

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class SortedBiMultiValMap<K, V> extends BiMultiValMap<K, V>
{
    private static final Comparator defaultComparator = (o1, o2) -> ((Comparable) o1).compareTo(o2);

    public static <K, V> SortedBiMultiValMap<K, V> create(Comparator<? super K> keyComparator, Comparator<? super V> valueComparator)
    {
        if (keyComparator == null)
            keyComparator = defaultComparator;
        if (valueComparator == null)
            valueComparator = defaultComparator;
        return new SortedBiMultiValMap<>(new TreeMap<>(keyComparator), new TreeMap<>(valueComparator));
    }

    public static <K, V> SortedBiMultiValMap<K, V> create(SortedBiMultiValMap<K, V> map)
    {
        Comparator<? super K> keyComparator = ((SortedMap<K, V>) (map.forwardMap)).comparator();
        Comparator<? super V> valueComparator = ((SortedMap<V, K>) (map.reverseMap)).comparator();
        SortedBiMultiValMap<K, V> instance = SortedBiMultiValMap.create(keyComparator, valueComparator);
        instance.forwardMap.putAll(map.forwardMap);
        for (Entry<V, Set<K>> entry : map.reverseMap.entrySet())
        {
            TreeSet<K> ks = new TreeSet<>(keyComparator);
            ks.addAll(entry.getValue());
            instance.reverseMap.put(entry.getKey(), ks);
        }

        return instance;
    }

    private SortedBiMultiValMap(Map<K, V> forwardMap, Map<V, Set<K>> reverseMap)
    {
        super(forwardMap, reverseMap);
    }
}
