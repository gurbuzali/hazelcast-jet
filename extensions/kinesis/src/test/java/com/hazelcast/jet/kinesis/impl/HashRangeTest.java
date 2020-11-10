/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis.impl;

import org.junit.Test;

import java.math.BigInteger;

import static com.hazelcast.jet.kinesis.impl.HashRange.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HashRangeTest {

    @Test
    public void partition() {
        HashRange range = of(1000, 2000);
        assertEquals(of(1000, 1500), range.partition(0, 2));
        assertEquals(of(1500, 2000), range.partition(1, 2));

        range = of("0", "170141183460469231731687303715884105728");
        assertEquals(of("0", "14178431955039102644307275309657008810"),
                range.partition(0, 12));
        assertEquals(of("14178431955039102644307275309657008810", "28356863910078205288614550619314017621"),
                range.partition(1, 12));
        assertEquals(of("28356863910078205288614550619314017621", "42535295865117307932921825928971026432"),
                range.partition(2, 12));
        // ...
        assertEquals(of("141784319550391026443072753096570088106", "155962751505430129087380028406227096917"),
                range.partition(10, 12));
        assertEquals(of("155962751505430129087380028406227096917", "170141183460469231731687303715884105728"),
                range.partition(11, 12));
    }

    @Test
    public void contains() {
        HashRange range = of(1000, 2000);
        assertFalse(range.contains(new BigInteger("999")));
        assertTrue(range.contains(new BigInteger("1000")));
        assertTrue(range.contains(new BigInteger("1999")));
        assertFalse(range.contains(new BigInteger("2000")));
        assertFalse(range.contains(new BigInteger("2001")));
    }

    @Test
    public void isAdjacent() {
        HashRange range = of(1000, 2000);
        assertFalse(range.isAdjacent(of(0, 999)));
        assertTrue(range.isAdjacent(of(0, 1000)));
        assertTrue(range.isAdjacent(of(2000, 3000)));
        assertFalse(range.isAdjacent(of(2001, 3000)));
    }

}
