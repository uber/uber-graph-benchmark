package com.uber.ugb;

import com.google.common.base.Preconditions;

import java.util.Random;

/**
 * A simple O(n) time, O(n) space random permutation generator which materializes the permutation and
 * inverse permutation as arrays
 */
class RandomPermutation {
    private final Random random;
    private int[] permutation;
    private int[] inversePermutation;

    RandomPermutation(final int size) {
        this(size, new Random());
    }

    RandomPermutation(final int size, final Random random) {
        this.random = random;
        Preconditions.checkArgument(size > 0);
        generate(size);
    }

    int[] getPermutation() {
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
