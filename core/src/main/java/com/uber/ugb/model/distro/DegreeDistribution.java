package com.uber.ugb.model.distro;

import java.io.Serializable;
import java.util.Random;

public interface DegreeDistribution extends Serializable {
    Sample createSample(int size, Random random);

    interface Sample {
        int getNextDegree();

        int getNextIndex();
    }
}
