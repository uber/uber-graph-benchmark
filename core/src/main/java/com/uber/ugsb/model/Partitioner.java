package com.uber.ugsb.model;

import com.uber.ugsb.GraphGenerator;
import com.uber.ugsb.schema.Vocabulary;

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

    public Map<String, GraphGenerator.IndexSet<Integer>> getPartition(final int total) {
        // use an order-preserving map
        Map<String, GraphGenerator.IndexSet<Integer>> partition = new LinkedHashMap<>();
        int remaining = total;
        float totalWeight = (float) findTotalWeight();
        int firstIndex = 0;
        for (Map.Entry<String, Integer> e : weightByLabel.entrySet()) {
            String label = e.getKey();
            int weight = Math.min(remaining, (int) (total * e.getValue() / totalWeight));
            GraphGenerator.IntervalSet vertices = new GraphGenerator.IntervalSet(firstIndex, weight);
            firstIndex += weight;
            remaining -= weight;
            partition.put(label, vertices);
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
