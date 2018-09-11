package com.uber.ugb.model.distro;

import com.google.common.base.Preconditions;
import com.uber.ugb.schema.Vocabulary;

import java.util.Random;

public class ConstantDegreeDistribution implements DegreeDistribution {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

    private final int degree;

    public ConstantDegreeDistribution(int degree) {
        // other constant degrees are not yet needed
        Preconditions.checkArgument(degree == 1);

        this.degree = degree;
    }

    @Override
    public Sample createSample(final int size, final Random random) {
        return new Sample() {
            private int nextIndex = 0;

            @Override
            public int getNextDegree() {
                return degree;
            }

            @Override
            public int getNextIndex() {
                return nextIndex++;
            }
        };
    }
}
