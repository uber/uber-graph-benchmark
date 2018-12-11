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

import org.apache.commons.math3.distribution.LogNormalDistribution;

import java.util.Random;

public class LogNormalDegreeDistribution implements DegreeDistribution {
    private static final long serialVersionUID = 3628732958875020385L;

    private final double meanLog;
    private final double sdLog;

    public LogNormalDegreeDistribution(double meanLog, double sdLog) {
        this.meanLog = meanLog;
        this.sdLog = sdLog;
    }

    @Override
    public Sample createSample(final int size, final Random random) {
        LogNormalDistribution distro = new LogNormalDistribution(meanLog, sdLog);
        distro.reseedRandomGenerator(random.nextLong());

        return new Sample() {
            @Override
            public int getNextDegree() {
                // note: can produce zero
                return (int) (0.5 + distro.sample());
            }

            @Override
            public int getNextIndex() {
                return (int) distro.inverseCumulativeProbability(random.nextDouble());
            }
        };
    }
}
