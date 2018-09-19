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
