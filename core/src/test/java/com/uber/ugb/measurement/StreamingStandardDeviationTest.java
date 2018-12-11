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

package com.uber.ugb.measurement;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class StreamingStandardDeviationTest {

    @Test
    public void testStandardDeviation() {
        StandardDeviation sd = new StandardDeviation(false);

        Random random = new Random();
        int total = 10000;
        double[] v = new double[total];
        for (int i = 0; i < total; i++) {
            v[i] = random.nextDouble();
        }
        double std = sd.evaluate(v);

        StreamingStandardDeviation ssd = new StreamingStandardDeviation();
        for (int i = 0; i < total; i++) {
            ssd.put(v[i]);
        }

        assertEquals("streaming std", std, ssd.getStandardDeviation(), 0.0001d);

    }
}
