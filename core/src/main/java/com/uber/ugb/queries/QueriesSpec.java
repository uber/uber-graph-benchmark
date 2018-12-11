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

package com.uber.ugb.queries;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

import java.util.Properties;

public class QueriesSpec {
    @JsonProperty("queries")
    public Query[] queries;

    public static class Query {
        @JsonProperty("name")
        public String name;
        @JsonProperty("type")
        public String type;
        @JsonProperty("startVertexLabel")
        public String startVertexLabel;

        @JsonProperty("queryText")
        public String queryText;

        @JsonProperty("queryType")
        public String queryType;

        @JsonProperty("steps")
        public Step[] steps;

        public static class Step {
            @JsonProperty("edge")
            public Edge edge;
            @JsonProperty("vertex")
            public Vertex vertex;

            public static Filter parseFilter(String filter) {
                if (Strings.isNullOrEmpty(filter)) {
                    return null;
                }
                String[] parts = filter.split("\\s+");
                if (parts.length != 3) {
                    return null;
                }

                if (parts[1].equals("=")) {
                    return new Filter(parts[0], Filter.Operator.Equal, parts[2]);
                }
                if (parts[1].equals(">")) {
                    return new Filter(parts[0], Filter.Operator.GreaterThan, parts[2]);
                }
                if (parts[1].equals("<")) {
                    return new Filter(parts[0], Filter.Operator.LessThan, parts[2]);
                }
                return null;
            }

            public static class Edge {
                @JsonProperty("label")
                public String label;
                @JsonProperty("select")
                public String select;
                @JsonProperty("filter")
                public String filter;
                @JsonProperty("limit")
                public int limit;
                @JsonProperty("orderByTs")
                public boolean orderByTs;
                @JsonProperty("direction")
                public String direction;

                private Filter filterObect;

                public Filter getFilter() {
                    if (filterObect == null) {
                        filterObect = parseFilter(filter);
                    }
                    return filterObect;
                }

                public boolean isBackward() {
                    return "in".equals(direction);
                }

                public boolean matchEdgeFilter(Properties properties) {
                    if (filter == null) {
                        return true;
                    }

                    if (properties == null) {
                        return false;
                    }

                    if (filterObect == null) {
                        filterObect = parseFilter(filter);
                    }
                    Object value = filterObect.getValueObject();

                    String propertyValue = properties.getProperty(filterObect.field);
                    switch (filterObect.operator) {
                        case Equal:
                            return value.equals(propertyValue);
                        case GreaterThan:
                            return (Integer) value - Integer.parseInt(propertyValue) > 0;
                        case LessThan:
                            return (Integer) value - Integer.parseInt(propertyValue) < 0;
                    }
                    return false;
                }

            }

            public static class Vertex {
                @JsonProperty("select")
                public String select;
            }

        }
    }

}
