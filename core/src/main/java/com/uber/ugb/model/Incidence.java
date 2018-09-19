package com.uber.ugb.model;

import com.uber.ugb.model.distro.DegreeDistribution;

import java.io.Serializable;

public class Incidence implements Serializable {
    private static final long serialVersionUID = -2672233051122503355L;

    private final String vertexLabel;
    private final double existenceProbability;
    private final DegreeDistribution degreeDistribution;

    public Incidence(final String vertexLabel,
                     final double existenceProbability,
                     final DegreeDistribution degreeDistribution) {
        this.vertexLabel = vertexLabel;
        this.existenceProbability = existenceProbability;
        this.degreeDistribution = degreeDistribution;
    }

    public String getVertexLabel() {
        return vertexLabel;
    }

    public double getExistenceProbability() {
        return existenceProbability;
    }

    public DegreeDistribution getDegreeDistribution() {
        return degreeDistribution;
    }
}
