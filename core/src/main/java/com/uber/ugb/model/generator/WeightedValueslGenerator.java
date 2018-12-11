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
