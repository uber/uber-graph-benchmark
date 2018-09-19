package com.uber.ugb.model.generator;

import com.uber.ugb.statistics.StatisticsSpec;

public class WeightedValueslGenerator extends Generator<String> {

    StatisticsSpec.PropertyValueWeight[] valueWeights;
    int totalWeights = 0;

    public WeightedValueslGenerator(StatisticsSpec.PropertyValueWeight[] valueWeights) {
        totalWeights = 0;
        this.valueWeights = valueWeights;
        for (StatisticsSpec.PropertyValueWeight valueWeight : valueWeights) {
            totalWeights += valueWeight.weight;
        }
    }

    @Override
    protected String genValue() {
        int x = random.nextInt(this.totalWeights);

        for (int i = 0; i < valueWeights.length; i++) {
            if (x < valueWeights[i].weight) {
                return valueWeights[i].value;
            }
            x -= valueWeights[i].weight;
        }

        return "";
    }
}
