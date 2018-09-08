package com.uber.ugsb.model.distro;

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
