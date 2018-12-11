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

import com.google.common.base.Preconditions;

import java.util.Random;

/**
 * A simple O(n) time, O(n) space random permutation generator which materializes the permutation and
 * inverse permutation as arrays
 */
public class RandomPermutation {
    private final Random random;
    private int[] permutation;
    private int[] inversePermutation;

    RandomPermutation(final int size) {
        this(size, new Random());
    }

    public RandomPermutation(final int size, final Random random) {
        this.random = random;
        Preconditions.checkArgument(size > 0);
        generate(size);
    }

    public int[] getPermutation() {
        return permutation;
    }

    int[] getInversePermutation() {
        return inversePermutation;
    }

    private void generate(final int size) {
        permutation = new int[size];
        inversePermutation = new int[size];
        for (int i = 0; i < size; i++) {
            permutation[i] = i;
        }
        int i = size;
        while (i > 0) {
            int pos = random.nextInt(i);
            i--;
            swap(permutation, i, pos);
            inversePermutation[permutation[i]] = i;
        }
    }

    private void swap(int[] array, int i, int j) {
        int tmp = array[i];
        array[i] = array[j];
        array[j] = tmp;
    }
}
