package com.uber.ugb.model.generator;

import com.uber.ugb.statistics.StatisticsSpec;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class WeightedValuesGeneratorTest {

    @Test
    public void testWeightedValuesGen() {
        WeightedValueslGenerator gen = new WeightedValueslGenerator(
            new StatisticsSpec.PropertyValueWeight[]{
                item("good", 1),
                item("bad", 1),
                item("unknown", 1),
            }
        );

        String x = (String) gen.generate("User", 1, "status");
        String y = (String) gen.generate("User", 1, "status");
        String z = (String) gen.generate("User", 1, "driverStatus");

        assertEquals(x, y);
        assertNotEquals(x, z);

        int total = 10000;

        Map<String, Integer> distribution = new HashMap<>();
        for (long k = 1; k < total; k++) {
            String result = (String) gen.generate("User", k, "status");
            int count = distribution.getOrDefault(result, 1);
            count++;
            distribution.put(result, count);
        }

        for (Map.Entry<String, Integer> entry : distribution.entrySet()) {
            assertTrue(Math.abs(entry.getValue() * 1.0f / total - 1.0 / 3.0) < 0.01);
        }

    }

    private StatisticsSpec.PropertyValueWeight item(String value, int weight) {
        StatisticsSpec.PropertyValueWeight t = new StatisticsSpec.PropertyValueWeight();
        t.value = value;
        t.weight = weight;
        return t;
    }
}
