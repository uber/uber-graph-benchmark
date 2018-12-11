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

import static org.junit.Assert.assertEquals;

public class LogNormalDegreeDistributionTest extends GraphGenTestBase {

    @Test
    public void logNormalSampleIsWithinBounds() throws Exception {
        double meanLog = 0.5715735;
        double sdLog = 0.5886170;
        Random random = new Random();
        LogNormalDegreeDistribution distro = new LogNormalDegreeDistribution(meanLog, sdLog);

        int n = 10000;
        DegreeDistribution.Sample sample = distro.createSample(n, random);

        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (int i = 0; i < n; i++) {
            double value = sample.getNextDegree();
            stats.addValue(value);
        }

        // possible (but very unlikely) to fail, as it is a random sample
        assertEquals(2.11, stats.getMean(), 0.5);
        assertEquals(1.35, stats.getStandardDeviation(), 0.5);

        //System.out.println("mean = " + stats.getMean());
        //System.out.println("sd = " + stats.getStandardDeviation());
    }

    @Test
    public void indexesFollowLogNormalDistribution() throws Exception {
        double meanLog = 0.5715735;
        double sdLog = 0.5886170;
        Random random = new Random();
        LogNormalDegreeDistribution distro = new LogNormalDegreeDistribution(meanLog, sdLog);

        int n = 1000;
        DegreeDistribution.Sample sample = distro.createSample(n, random);

        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (int i = 0; i < n; i++) {
            int index = sample.getNextIndex();
            stats.addValue(index);
        }

        // possible (but very unlikely) to fail, as it is a random sample
        assertEquals(1.6, stats.getMean(), 0.5);
    }

    @Ignore
    @Test
    public void verifyDistributionsManuallyInR() throws IOException {
        double meanLog = 0.5715735;
        double sdLog = 0.5886170;
        Random random = new Random();
        LogNormalDegreeDistribution distro = new LogNormalDegreeDistribution(meanLog, sdLog);
        int n = 10000;
        DegreeDistribution.Sample sample = distro.createSample(n, random);

        writeTo(new File("/tmp/distro.csv"), ps -> {
            ps.println("degree");
            for (int i = 0; i < n; i++) {
                ps.println(sample.getNextDegree());
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
