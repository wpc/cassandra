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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A variant of BiMap which does not enforce uniqueness of values. This means the inverse
 * is a Multimap.  (But the "forward" view is not a multimap; keys may only each have one value.)
 *
 * @param <K>
 * @param <V>
 */
public class BiMultiValMap<K, V> implements Map<K, V>
{
    private static final int DEFAULT_VALUES_PER_KEY = 2;

    protected final Map<K, V> forwardMap;
    protected final Map<V, Set<K>> reverseMap;

    public BiMultiValMap()
    {
        this.forwardMap = new HashMap<>();
        this.reverseMap = new HashMap<>();
    }

    protected BiMultiValMap(Map<K, V> forwardMap, Map<V, Set<K>> reverseMap)
    {
        this.forwardMap = forwardMap;
        this.reverseMap = reverseMap;
    }

    public BiMultiValMap(BiMultiValMap<K, V> map)
    {
        this();
        forwardMap.putAll(map.forwardMap);
        // make sure Set in the reverse map is recreated
        for (Entry<V, Set<K>> entry : map.reverseMap.entrySet())
        {
            reverseMap.put(entry.getKey(), createSet(entry.getValue()));
        }
    }

    private Set<K> createSet(Set<K> source)
    {
        if (forwardMap instanceof SortedMap)
        {
            TreeSet<K> set = new TreeSet<>(((SortedMap<K, V>) forwardMap).comparator());
            set.addAll(source);
            return set;
        }

        return new HashSet<>(source);
    }

    private Set<K> createSet()
    {
        if (forwardMap instanceof SortedMap)
            return new TreeSet<>(((SortedMap<K, V>) forwardMap).comparator());

        return new HashSet<>(DEFAULT_VALUES_PER_KEY);
    }

    public void clear()
    {
        forwardMap.clear();
        reverseMap.clear();
    }

    public boolean containsKey(Object key)
    {
        return forwardMap.containsKey(key);
    }

    public boolean containsValue(Object value)
    {
        return reverseMap.containsKey(value);
    }

    public Set<Map.Entry<K, V>> entrySet()
    {
        return forwardMap.entrySet();
    }

    public V get(Object key)
    {
        return forwardMap.get(key);
    }

    public boolean isEmpty()
    {
        return forwardMap.isEmpty();
    }

    public Set<K> keySet()
    {
        return Collections.unmodifiableSet(forwardMap.keySet());
    }

    public V put(K key, V value)
    {
        V oldVal = forwardMap.put(key, value);
        if (oldVal != null)
        {
            Set<K> keys = reverseMap.get(oldVal);
            if (keys != null)
            {
                keys.remove(key);
            }
        }
        Set<K> keys = reverseMap.get(value);
        if (keys == null)
        {
            keys = createSet();
            reverseMap.put(value, keys);
        }
        keys.add(key);
        return oldVal;
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public V remove(Object key)
    {
        V oldVal = forwardMap.remove(key);
        Set<K> keys = reverseMap.get(oldVal);
        if (keys != null)
            keys.remove(key);
        return oldVal;
    }

    public Collection<K> removeValue(V value)
    {
        Set<K> keys = reverseMap.remove(value);
        if (keys == null)
        {
            return Collections.emptySet();
        }
        for (K key : keys)
            forwardMap.remove(key);
        return Collections.unmodifiableSet(keys);
    }

    public int size()
    {
        return forwardMap.size();
    }

    public Collection<V> values()
    {
        return forwardMap.values();
    }

    public Set<V> valueSet()
    {
        return Collections.unmodifiableSet(reverseMap.keySet());
    }

    public Collection<K> inverseGet(V value)
    {
        Set<K> keys = reverseMap.get(value);
        if (keys == null)
        {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(keys);
    }
}
