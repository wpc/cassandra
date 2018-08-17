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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SortedBiMultiValMapTest
{
    private SortedBiMultiValMap<String, String> bimap;

    @Before
    public void setup()
    {
        bimap = SortedBiMultiValMap.create(null, null);
        bimap.put("c", "1");
        bimap.put("b", "2");
        bimap.put("a", "1");
    }

    @Test
    public void testKeysAndValuesAreInOrder()
    {
        assertEquals(Arrays.asList("a", "b", "c"), new ArrayList<>(bimap.keySet()));
        assertEquals(Arrays.asList("a", "c"), new ArrayList<>(bimap.inverseGet("1")));
    }

    @Test
    public void testCopySortedBiMap()
    {
        SortedBiMultiValMap<String, String> copied = SortedBiMultiValMap.create(bimap);
        // copied should have same data
        assertEquals(bimap.size(), copied.size());
        assertEquals(Arrays.asList("a", "b", "c"), new ArrayList<>(copied.keySet()));
        assertEquals(Arrays.asList("a", "c"), new ArrayList<>(copied.inverseGet("1")));

        //modify copied should not modify original
        copied.remove("c");
        assertEquals("1", bimap.get("c"));
        assertEquals(Arrays.asList("a", "c"), new ArrayList<>(bimap.inverseGet("1")));
    }

    @Test
    public void testCustomComparators()
    {
        SortedBiMultiValMap<String, String> map = SortedBiMultiValMap.create(Comparator.<String>reverseOrder(), Comparator.<String>reverseOrder());
        map.put("c", "1");
        map.put("b", "2");
        map.put("a", "1");
        assertEquals(Arrays.asList("c", "b", "a"), new ArrayList<>(map.keySet()));
        assertEquals(Arrays.asList("c", "a"), new ArrayList<>(map.inverseGet("1")));
    }

    @Test
    public void testCopyShouldKeptComparator()
    {
        SortedBiMultiValMap<String, String> map = SortedBiMultiValMap.create(Comparator.<String>reverseOrder(), Comparator.<String>reverseOrder());
        map.put("c", "1");
        map.put("b", "2");
        map.put("a", "1");
        SortedBiMultiValMap<String, String> copied = SortedBiMultiValMap.create(map);
        assertEquals(Arrays.asList("c", "b", "a"), new ArrayList<>(copied.keySet()));
        assertEquals(Arrays.asList("c", "a"), new ArrayList<>(copied.inverseGet("1")));
    }
}