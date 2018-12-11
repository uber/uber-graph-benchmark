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

package com.uber.ugb.model.distro;

import org.apache.commons.math3.distribution.UniformRealDistribution;

import java.util.Random;

public class UniformDegreeDistribution implements DegreeDistribution {
    private static final long serialVersionUID = -9182380389251181103L;

    @Override
    public Sample createSample(final int size, final Random random) {
        final UniformRealDistribution distro = new UniformRealDistribution(0, size);
        distro.reseedRandomGenerator(random.nextLong());

        return new Sample() {
            @Override
            public int getNextIndex() {
                return (int) distro.inverseCumulativeProbability(random.nextDouble());
            }

            @Override
            public int getNextDegree() {
                return (int) distro.sample();
            }
        };
    }
}
