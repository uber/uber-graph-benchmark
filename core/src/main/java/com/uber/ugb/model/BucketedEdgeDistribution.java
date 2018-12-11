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

package com.uber.ugb.model;

import com.google.common.base.Preconditions;
import com.uber.ugb.model.distro.DegreeDistribution;
import com.uber.ugb.util.RandomPermutation;

import java.util.Arrays;
import java.util.Random;

public class BucketedEdgeDistribution {

    RandomSubset<Integer> domainSubset;
    int domainBucketWidth;
    WeightedBuckets domainWeightedBuckets;
    Random random;

    public BucketedEdgeDistribution(Incidence incidence, long domainSize, final Random random) {
        domainBucketWidth = domainSize > 1024 * 1024 ? 1024 : 1;
        int domainBucketCount = (int) (domainSize / domainBucketWidth);
        domainSubset = createRandomSubset(
            new DirectSet(domainBucketCount), incidence.getExistenceProbability(), random);
        domainWeightedBuckets = incidence.getDegreeDistribution(domainSubset.size(), random);
        this.random = random;
    }

    public long pickOne() {
        int domainBucket = domainWeightedBuckets.locate(random);
        int tbd = random.nextInt(domainBucketWidth);
        long tailIndex = (long) domainSubset.get(domainBucket) * domainBucketWidth + tbd;
        return tailIndex;
    }

    private <T> RandomSubset<T> createRandomSubset(final IndexSet<T> base,
                                                   final double probability, final Random random) {
        int size = (int) (probability * base.size());
        return new RandomSubset<>(base, size, random);
    }


    public interface IndexSet<T> {
        int size();

        T get(int index);
    }

    public static class RandomSubset<T> implements IndexSet<T> {
        private final IndexSet<T> base;
        private final int[] permutation;
        private final int size;

        RandomSubset(final IndexSet<T> base, final int size, final Random random) {
            this.base = base;
            this.permutation = new RandomPermutation(base.size(), random).getPermutation();
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public T get(final int index) {
            Preconditions.checkArgument(index < size);
            return base.get(permutation[index]);
        }
    }

    public static class IntervalSet implements IndexSet<Integer> {
        private final int firstIndex;
        private final int size;

        public IntervalSet(int firstIndex, int size) {
            this.firstIndex = firstIndex;
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Integer get(int index) {
            Preconditions.checkArgument(index < size);
            return firstIndex + index;
        }
    }

    public static class DirectSet implements IndexSet<Integer> {
        private final int size;

        public DirectSet(int size) {
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Integer get(int index) {
            Preconditions.checkArgument(index < size);
            return index;
        }
    }

    public static class WeightedBuckets {
        double[] accumulatedWeights;
        double totalWeight;

        public WeightedBuckets(DegreeDistribution.Sample sample, int bucketCount) {
            double[] weights = new double[bucketCount];
            int[] degrees = new int[bucketCount];
            for (int i = 0; i < bucketCount; i++) {
                degrees[i] = sample.getNextDegree();
            }
            init(degrees, weights);
        }

        public WeightedBuckets(int[] degrees, double[] weights) {
            init(degrees, weights);
        }

        private void init(int[] degrees, double[] weights) {

            Preconditions.checkArgument(degrees.length == weights.length);

            accumulatedWeights = new double[weights.length];
            totalWeight = 0;
            for (int i = 0; i < degrees.length; i++) {
                weights[i] = degrees[i];
                totalWeight += weights[i];
            }
            double currentWeight = 0;
            for (int i = 0; i < weights.length; i++) {
                currentWeight += weights[i];
                accumulatedWeights[i] = currentWeight / totalWeight;
            }
        }

        public int locate(Random random) {
            double r = random.nextDouble();
            int x = Arrays.binarySearch(this.accumulatedWeights, r);
            if (x < 0) {
                x = (-x) - 1;
            }
            return x;
        }

        public double getTotalWeight() {
            return totalWeight;
        }
    }
}
