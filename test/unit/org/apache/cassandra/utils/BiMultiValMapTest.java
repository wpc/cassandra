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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class BiMultiValMapTest
{
    private BiMultiValMap<String, String> bimap;

    @Before
    public void setup()
    {
        bimap = new BiMultiValMap<>();
        bimap.put("a", "1");
        bimap.put("b", "2");
        bimap.put("c", "1");
    }

    @Test
    public void testGet()
    {
        assertEquals("1", bimap.get("a"));
        assertEquals("1", bimap.get("c"));
        assertEquals("2", bimap.get("b"));
        assertNull(bimap.get("d"));
        assertNull(bimap.get(null));
    }

    @Test
    public void testInverseGet()
    {
        assertEquals(setOf("a", "c"), bimap.inverseGet("1"));
        assertEquals(setOf("b"), bimap.inverseGet("2"));
        assertEquals(setOf(), bimap.inverseGet("3"));
    }

    @Test
    public void testDuplicatePut()
    {
        assertEquals("1", bimap.put("a", "1"));
        assertEquals("1", bimap.get("a"));
        assertEquals(setOf("a", "c"), bimap.inverseGet("1"));
    }

    @Test
    public void testReplaceValue()
    {
        assertEquals("1", bimap.put("c", "2"));
        assertEquals("2", bimap.get("c"));
        assertEquals(setOf("a"), bimap.inverseGet("1"));
        assertEquals(setOf("b", "c"), bimap.inverseGet("2"));
    }

    @Test
    public void testPutAll()
    {
        BiMultiValMap<String, String> another = new BiMultiValMap<>();
        another.put("c", "2");
        another.put("d", "1");
        bimap.putAll(another);
        assertEquals(4, bimap.size());
        assertEquals("2", bimap.get("c"));
        assertEquals("1", bimap.get("d"));
        assertEquals(setOf("a", "d"), bimap.inverseGet("1"));
        assertEquals(setOf("b", "c"), bimap.inverseGet("2"));
    }

    @Test
    public void testContainKey()
    {
        assertTrue(bimap.containsKey("a"));
        assertTrue(bimap.containsKey("b"));
        assertTrue(bimap.containsKey("c"));
        assertFalse(bimap.containsKey("d"));
        assertFalse(bimap.containsKey("1"));
        assertFalse(bimap.containsKey("2"));
        assertFalse(bimap.containsKey(null));
    }

    @Test
    public void testContainValue()
    {
        assertTrue(bimap.containsValue("1"));
        assertTrue(bimap.containsValue("2"));
        assertFalse(bimap.containsValue("a"));
        assertFalse(bimap.containsValue("b"));
        assertFalse(bimap.containsValue("c"));
        assertFalse(bimap.containsValue(null));
    }

    @Test
    public void testRemove()
    {
        assertEquals("1", bimap.remove("a"));
        assertFalse(bimap.containsKey("a"));
        assertEquals(setOf("c"), bimap.inverseGet("1"));
        bimap.remove("c");
        assertEquals(setOf(), bimap.inverseGet("1"));
    }

    @Test
    public void testRemoveValue()
    {
        assertEquals(setOf("a", "c"), bimap.removeValue("1"));
        assertEquals(setOf(), bimap.inverseGet("1"));
        assertEquals(setOf("b"), bimap.inverseGet("2"));
        assertEquals(1, bimap.size());
        assertEquals(setOf("b"), bimap.removeValue("2"));
        assertTrue(bimap.isEmpty());
    }

    @Test
    public void testKeySet()
    {
        assertEquals(setOf("a", "b", "c"), bimap.keySet());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNotSupportModificationViaKeySet()
    {
        bimap.keySet().remove("c");
    }

    @Test
    public void testGetValuesView()
    {
        ArrayList<String> values = new ArrayList<>(bimap.values());
        Collections.sort(values);
        assertEquals(Arrays.asList("1", "1", "2"), values);
    }

    @Test
    public void testGetValueSet()
    {
        assertEquals(setOf("1", "2"), bimap.valueSet());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNotSupportModificationViaValueSet()
    {
        bimap.valueSet().remove("1");
    }

    @Test
    public void testCopy()
    {
        BiMultiValMap<String, String> copied = new BiMultiValMap<>(bimap);
        // copied should have same data
        assertEquals(bimap.size(), copied.size());
        assertEquals(bimap.get("a"), copied.get("a"));
        assertEquals(bimap.get("c"), copied.get("c"));
        assertEquals(bimap.get("b"), copied.get("b"));
        assertEquals(bimap.inverseGet("1"), copied.inverseGet("1"));
        assertEquals(bimap.inverseGet("2"), copied.inverseGet("2"));
        assertEquals(bimap.keySet(), copied.keySet());
        assertEquals(bimap.valueSet(), copied.valueSet());
        //modify copied should not modify original
        copied.remove("c");
        assertEquals("1", bimap.get("c"));
        assertEquals(setOf("a", "c"), bimap.inverseGet("1"));
    }

    @Test
    public void testClear()
    {
        bimap.clear();
        assertEquals(0, bimap.size());
        assertTrue(bimap.isEmpty());
        assertNull(bimap.get("a"));
        assertNull(bimap.get("b"));
        assertNull(bimap.get("c"));
        assertEquals(setOf(), bimap.inverseGet("1"));
        assertEquals(setOf(), bimap.inverseGet("2"));
    }

    private <T> Set<T> setOf(T... elements)
    {
        return new HashSet<T>(Arrays.asList(elements));
    }
}