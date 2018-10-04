package com.uber.ugb.db.mock;

import com.uber.ugb.db.AbstractSubgraphDB;
import com.uber.ugb.db.Status;
import com.uber.ugb.db.Subgraph;
import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MockMemAbstractSubgraphDB extends AbstractSubgraphDB {

    private static final String REVERSE_SUFFIX = "_r";

    private Map<Long, Vertex> vertices;

    public MockMemAbstractSubgraphDB() {
        this.vertices = new HashMap<>();
    }

    @Override
    public Status writeVertex(QualifiedName label, Object id, Object... keyValues) {
        Long vid = (Long) id;

        Vertex vertex = new Vertex(label, vid, toProperties(keyValues));
        this.vertices.put(vid, vertex);
        return Status.OK;
    }

    @Override
    public Status writeEdge(QualifiedName edgeLabel,
                            QualifiedName outVertexLabel, Object outVertexId,
                            QualifiedName inVertexLabel, Object inVertexId,
                            Object... keyValues) {
        Vertex outVertex = vertices.get(outVertexId);
        Vertex inVertex = vertices.get(inVertexId);

        Properties edgeProperties = toProperties(keyValues);
        outVertex.addEdge(edgeLabel, (long) inVertexId, edgeProperties);
        inVertex.addEdge(new QualifiedName(edgeLabel.toString() + REVERSE_SUFFIX), (long) outVertexId, edgeProperties);
        return Status.OK;
    }

    @Override
    public Properties readVertex(QualifiedName label, Object id, QueriesSpec.Query.Step.Vertex vertexQuerySpec) {
        Vertex vertex = vertices.get(id);
        return vertex == null ? null : extractProperties(vertex.properties, vertexQuerySpec.select, null);
    }

    @Override
    public List<Subgraph.Edge> readEdges(Object startVertexId, QueriesSpec.Query.Step.Edge edgeQuerySpec) {
        String edgeLabel = !edgeQuerySpec.isBackward() ? edgeQuerySpec.label : edgeQuerySpec.label + REVERSE_SUFFIX;
        Vertex startVertex = vertices.get(startVertexId);
        if (startVertex == null) {
            return new ArrayList<>();
        }
        List<Subgraph.Edge> foundEdges = new ArrayList<>();
        List<Edge> edgeList = startVertex.edges.getOrDefault(new QualifiedName(edgeLabel), new ArrayList<>());
        for (Edge edge : edgeList) {
            Subgraph.Edge foundEdge = new Subgraph.Edge(startVertexId, edge.nextVertexId, edge.edgeProperties);
            foundEdges.add(foundEdge);
        }
        return foundEdges;
    }

    private Properties toProperties(Object[] keyValues) {
        Properties properties = new Properties();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            properties.put(keyValues[i], keyValues[i + 1]);
        }
        return properties;
    }

    private static class Edge {
        long nextVertexId;
        Properties edgeProperties;

        Edge(long nextVertexId, Properties edgeProperties) {
            this.nextVertexId = nextVertexId;
            this.edgeProperties = edgeProperties;
        }
    }

    private static class Vertex {
        long id;
        QualifiedName label;
        Properties properties;
        Map<QualifiedName, List<Edge>> edges;

        Vertex(QualifiedName label, long id, Properties properties) {
            this.id = id;
            this.label = label;
            this.properties = properties;
            this.edges = new HashMap<>();
        }

        void addEdge(QualifiedName egdeLabel, long otherVertexId, Properties properties) {
            List<Edge> adjacencyList = this.edges.computeIfAbsent(egdeLabel, k -> {
                return new ArrayList<>();
            });
            adjacencyList.add(new Edge(otherVertexId, properties));
        }

    }

}
