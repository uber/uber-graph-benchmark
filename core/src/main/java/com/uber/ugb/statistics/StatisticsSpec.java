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
        public int weight;
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
