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
