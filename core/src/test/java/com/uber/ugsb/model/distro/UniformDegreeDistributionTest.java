package com.uber.ugsb.model.distro;

import com.uber.ugsb.GraphGenTestBase;
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
