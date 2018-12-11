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

import com.uber.ugb.GraphGenTestBase;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class UniformDegreeDistributionTest extends GraphGenTestBase {

    @Ignore
    @Test
    public void verifyDistributionsManuallyInR() throws IOException {
        Random random = new Random();
        UniformDegreeDistribution distro = new UniformDegreeDistribution();

        int n = 10000;
        DegreeDistribution.Sample sample = distro.createSample(n, random);

        DescriptiveStatistics stats = new DescriptiveStatistics();

        writeTo(new File("/tmp/distro.csv"), ps -> {
            ps.println("degree");
            for (int i = 0; i < n; i++) {
                double value = sample.getNextDegree();
                ps.println(value);
                stats.addValue(value);
            }
        });

        writeTo(new File("/tmp/distro-inv-cuml.csv"), ps -> {
            ps.println("index");
            for (int i = 0; i < n; i++) {
                ps.println(sample.getNextIndex());
            }
        });
    }
}
