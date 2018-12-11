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

package com.uber.ugb.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Subgraph is the subgraph query result.
 * The results can be collected as edges via addEdge(Edge edge) function,
 * or just set the vertex and edge counts.
 */
public class Subgraph {
    public final Object startVertexId;
    public final List<Edge> edges;
    private int vertexCount;
    private int edgeCount;

    public Subgraph(Object startVertexId) {
        this.startVertexId = startVertexId;
        this.edges = Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * addEdge adds one found edge in the subgraph
     *
     * @param edge
     */
    public void addEdge(Edge edge) {
        this.edges.add(edge);
    }

    public Edge findEdge(Object knownVertexId, Object nextVertexId) {
        for (Edge edge : edges) {
            if (edge.knownVertexId.equals(knownVertexId) && edge.nextVertexId.equals(nextVertexId)) {
                return edge;
            }
        }
        return null;
    }

    public int getVertexCount() {
        if (this.edges.size() > 0) {
            return countVertices(this.edges);
        }
        return vertexCount;
    }

    public void setVertexCount(int vertexCount) {
        this.vertexCount = vertexCount;
    }

    private int countVertices(List<Edge> edges) {
        Set set = new HashSet();
        for (Edge edge : edges) {
            set.add(edge.knownVertexId);
            set.add(edge.nextVertexId);
        }
        return set.size();
    }

    public int getEdgeCount() {
        if (this.edges.size() > 0) {
            return countEdges(this.edges);
        }
        return edgeCount;
    }

    public void setEdgeCount(int edgeCount) {
        this.edgeCount = edgeCount;
    }

    private int countEdges(List<Edge> edges) {
        Set set = new HashSet();
        for (Edge edge : edges) {
            set.add(edge);
        }
        return set.size();
    }

    public static class Edge {
        public final Object knownVertexId;
        public final Object nextVertexId;
        public final Properties edgeProperties;
        protected Properties vertexProperties;

        public Edge(Object knownVertexId, Object nextVertexId,
                    Properties edgeProperties) {
            this.knownVertexId = knownVertexId;
            this.nextVertexId = nextVertexId;
            this.edgeProperties = edgeProperties;
        }

        public Properties getVertexProperties() {
            return vertexProperties;
        }

        public void setVertexProperties(Properties vertexProperties) {
            this.vertexProperties = vertexProperties;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Edge)) {
                return false;
            }
            Edge edge = (Edge) o;
            return Objects.equals(knownVertexId, edge.knownVertexId)
                && Objects.equals(nextVertexId, edge.nextVertexId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(knownVertexId, nextVertexId);
        }
    }
}
