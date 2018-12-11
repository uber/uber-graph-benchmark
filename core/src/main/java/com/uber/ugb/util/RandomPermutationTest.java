/*
 *
 *  * Copyright 2018 Uber Technologies Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.uber.ugb.util;

import com.uber.ugb.util.RandomPermutation;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RandomPermutationTest {

    @Test
    public void trivialPermutationIsCorrect() {
        RandomPermutation perm = new RandomPermutation(1);
        assertEquals(1, perm.getPermutation().length);
        assertEquals(1, perm.getInversePermutation().length);
        assertEquals(0, perm.getPermutation()[0]);
        assertEquals(0, perm.getInversePermutation()[0]);
    }

    @Test
    public void nontrivialPermutationIsCorrect() {
        for (int rep = 0; rep < 10; rep++) {
            int size = new Random().nextInt(99) + 1;
            RandomPermutation perm = new RandomPermutation(size);
            assertEquals(size, perm.getPermutation().length);
            assertEquals(size, perm.getInversePermutation().length);
            assertValuesUniqueAndBounded(perm.getPermutation());
            assertValuesUniqueAndBounded(perm.getInversePermutation());
            for (int i = 0; i < perm.getPermutation().length; i++) {
                assertEquals(i, perm.getInversePermutation()[perm.getPermutation()[i]]);
                assertEquals(i, perm.getPermutation()[perm.getInversePermutation()[i]]);
            }
        }
    }

    // cat /tmp/out.txt | sed 's/.*size //' | sed 's/ --> /,/' | sed 's/ .*//' > /tmp/times.csv
    // this is indeed linear, with about two minutes per billion items
    @Ignore
    @Test
    public void testInitializationTime() {
        Mutable<Integer> size = new Mutable<>(1);
        while (true) { // exit by failing with OOME
            long time = timeToEvaluate(() -> new RandomPermutation(size.value));
            System.out.println("permutation of size " + size.value + " --> " + time + " ms to construct");
            size.value *= 10;
        }
    }

    private long timeToEvaluate(final Runnable task) {
        long startTime = System.currentTimeMillis();
        task.run();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    private void assertValuesUniqueAndBounded(final int[] array) {
        Set<Integer> values = new HashSet<>();
        for (int x : array) {
            assertTrue(x >= 0);
            assertTrue(x < array.length);
            values.add(x);
        }
        assertEquals(array.length, values.size());
    }

    private static class Mutable<T> {
        private T value;

        Mutable(final T value) {
            this.value = value;
        }
    }
}
