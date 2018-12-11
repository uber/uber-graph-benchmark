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

package com.uber.ugb.model;

import com.uber.ugb.schema.QualifiedName;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Partitioner implements Serializable {
    private static final long serialVersionUID = -9209501217167191765L;

    // order-preserving for the sake of serialization
    private Map<QualifiedName, Float> weightByLabel = new LinkedHashMap<>();

    public void put(final QualifiedName label, final float weight) {
        weightByLabel.put(label, weight);
    }

    public Map<QualifiedName, Long> getPartitionSizes(final long total) {
        // use an order-preserving map
        Map<QualifiedName, Long> partition = new LinkedHashMap<>();
        long remaining = total;
        float totalWeight = (float) findTotalWeight();
        for (Map.Entry<QualifiedName, Float> e : weightByLabel.entrySet()) {
            QualifiedName label = e.getKey();
            long weight = Math.min(remaining, (long) (total * (e.getValue() * 1.0f / totalWeight)));
            remaining -= weight;
            partition.put(label, weight);
        }
        return partition;
    }

    public Set<QualifiedName> getLabels() {
        return weightByLabel.keySet();
    }

    public Map<QualifiedName, Float> getWeightByLabel() {
        return weightByLabel;
    }

    private float findTotalWeight() {
        float total = 0;
        for (Float weight : weightByLabel.values()) {
            total += weight;
        }
        return total;
    }
}
