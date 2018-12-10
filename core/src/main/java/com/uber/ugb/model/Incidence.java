package com.uber.ugb.model;

import com.uber.ugb.model.distro.DegreeDistribution;
import com.uber.ugb.schema.QualifiedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Logger;

public class Incidence implements Serializable {
    private static final long serialVersionUID = -2672233051122503355L;

    private static Logger logger = Logger.getLogger(Incidence.class.getName());

    private final QualifiedName vertexLabel;
    private final double existenceProbability;
    private DegreeDistribution degreeDistribution;
    private BucketedEdgeDistribution.WeightedBuckets weightedBuckets;

    public Incidence(final QualifiedName vertexLabel,
                     final double existenceProbability,
                     final DegreeDistribution degreeDistribution) {
        this.vertexLabel = vertexLabel;
        this.existenceProbability = existenceProbability;
        this.degreeDistribution = degreeDistribution;
    }

    public Incidence(final QualifiedName vertexLabel,
                     final double existenceProbability,
                     final String csvContent, final String direction, final String edgeLabel) {
        this.vertexLabel = vertexLabel;
        this.existenceProbability = existenceProbability;

        List<DegreeCount> counts = parseCsv(csvContent, direction);
        int[] degrees = new int[counts.size()];
        double[] weights = new double[counts.size()];
        int i = 0;
        for (i = 0; i < counts.size(); i++) {
            DegreeCount degreeCount = counts.get(i);
            degrees[i] = degreeCount.degree;
            weights[i] = degreeCount.count;
            if (degreeCount.degree == 0) {
                logger.severe(String.format("unexpected zero degree count for vertex %s direction %s in %s.csv",
                    vertexLabel.toString(), direction, edgeLabel));
            }
        }
        this.weightedBuckets = new BucketedEdgeDistribution.WeightedBuckets(degrees, weights);
    }

    public static List<DegreeCount> parseCsv(String csvContent, String direction) {
        List<DegreeCount> counts = new ArrayList<>();
        Scanner scanner = new Scanner(csvContent);
        scanner.useDelimiter(",");
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] parts = line.split(",");
            if (parts.length != 3) {
                continue;
            }
            if (direction.equals(parts[0])) {
                int degree = Integer.parseInt(parts[1]);
                long count = Long.parseLong(parts[2]);
                counts.add(new DegreeCount(degree, count));
            }
        }
        scanner.close();
        return counts;
    }

    public QualifiedName getVertexLabel() {
        return vertexLabel;
    }

    public double getExistenceProbability() {
        return existenceProbability;
    }

    public BucketedEdgeDistribution.WeightedBuckets getDegreeDistribution(int size, final Random random) {
        if (weightedBuckets == null) {
            DegreeDistribution.Sample domainSample = degreeDistribution.createSample(size, random);
            return new BucketedEdgeDistribution.WeightedBuckets(domainSample, size);
        }
        return weightedBuckets;
    }

    public static class DegreeCount {

        public final int degree;
        public final long count;

        public DegreeCount(int degree, long count) {
            this.degree = degree;
            this.count = count;
        }
    }
}
