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

import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ConstantDegreeDistributionTest {

    @Test
    public void constantSampleIsCorrect() throws Exception {
        ConstantDegreeDistribution distro = new ConstantDegreeDistribution(1);
        int n = 10;
        Random random = new Random();
        DegreeDistribution.Sample sample = distro.createSample(n, random);
        Set<Integer> indexes = new HashSet<>();
        for (int i = 0; i < n; i++) {
            assertEquals(1, sample.getNextDegree());
            indexes.add(sample.getNextIndex());
        }
        assertEquals(n, indexes.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void higherDegreeIsRejected() {
        new ConstantDegreeDistribution(2);
    }
}
