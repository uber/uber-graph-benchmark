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
