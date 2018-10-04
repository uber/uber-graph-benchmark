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
