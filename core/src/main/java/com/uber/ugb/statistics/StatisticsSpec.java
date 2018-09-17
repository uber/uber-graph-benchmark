package com.uber.ugb.statistics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.ugb.model.distro.ConstantDegreeDistribution;
import com.uber.ugb.model.distro.DegreeDistribution;
import com.uber.ugb.model.distro.LogNormalDegreeDistribution;

public class StatisticsSpec {
    @JsonProperty("vertices")
    public VertexWeight[] vertices;
    @JsonProperty("properties")
    public PropertyValues[] properties;
    @JsonProperty("edges")
    public EdgeDistribution[] edges;

    public static class VertexWeight {
        @JsonProperty("type")
        public String type;
        @JsonProperty("weight")
        public float weight;
    }

    public static class PropertyValues {
        @JsonProperty("type")
        public String type;
        @JsonProperty("values")
        public PropertyValueWeight[] propertyValueWeights;
    }

    public static class PropertyValueWeight {
        @JsonProperty("value")
        public String value;
        @JsonProperty("weight")
        public float weight;
    }

    public static class EdgeDistribution {
        @JsonProperty("type")
        public String type;
        @JsonProperty("out")
        public Distribution out;
        @JsonProperty("in")
        public Distribution in;
    }

    public static class Distribution {
        @JsonProperty("existenceProbability")
        public float existenceProbability;
        @JsonProperty("logMean")
        public float logMean;
        @JsonProperty("logSD")
        public float logSD;

        public DegreeDistribution toDegreeDistribution() {
            if (Math.abs(logMean) < 0.0001 && Math.abs(logSD) < 0.0001) {
                // this is zero
                return new ConstantDegreeDistribution(1);
            }
            return new LogNormalDegreeDistribution(logMean, logSD);
        }
    }

}
