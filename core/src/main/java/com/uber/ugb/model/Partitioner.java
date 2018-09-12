package com.uber.ugb.model;

import com.uber.ugb.schema.Vocabulary;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Partitioner implements Serializable {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

    // order-preserving for the sake of serialization
    private Map<String, Integer> weightByLabel = new LinkedHashMap<>();

    public void put(final String label, final int weight) {
        weightByLabel.put(label, weight);
    }

    public Map<String, Long> getPartitionSizes(final long total) {
        // use an order-preserving map
        Map<String, Long> partition = new LinkedHashMap<>();
        long remaining = total;
        float totalWeight = (float) findTotalWeight();
        for (Map.Entry<String, Integer> e : weightByLabel.entrySet()) {
            String label = e.getKey();
            long weight = Math.min(remaining, (long) (total * (e.getValue() * 1.0f / totalWeight)));
            remaining -= weight;
            partition.put(label, weight);
        }
        return partition;
    }

    public Set<String> getLabels() {
        return weightByLabel.keySet();
    }

    public Map<String, Integer> getWeightByLabel() {
        return weightByLabel;
    }

    private int findTotalWeight() {
        int total = 0;
        for (Integer weight : weightByLabel.values()) {
            total += weight;
        }
        return total;
    }
}
