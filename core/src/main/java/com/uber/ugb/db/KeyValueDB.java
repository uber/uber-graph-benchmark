package com.uber.ugb.db;

import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.storage.KeyValueStore;
import org.nustaq.serialization.FSTConfiguration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KeyValueDB extends AbstractSubgraphDB {

    private static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
    private static String Separator = ":";
    private static String REVERSE_SUFFIX = "_r";

    static {
        conf.registerClass(Properties.class);
        conf.registerClass(Edge.class);
        conf.registerClass(ArrayList.class);
    }

    private KeyValueStore kvs;

    public KeyValueDB() {
    }

    public void setKeyValueStore(KeyValueStore kvs) {
        this.kvs = kvs;
    }

    @Override
    public Properties readVertex(QualifiedName label, Object id, QueriesSpec.Query.Step.Vertex vertexQuerySpec) {
        byte[] value = kvs.get(genVertexKey(label, id));
        if (value == null) {
            return new Properties();
        }
        return (Properties) conf.asObject(value);
    }

    @Override
    public List<Subgraph.Edge> readEdges(Object startVertexId, QueriesSpec.Query.Step.Edge edgeQuerySpec) {
        byte[] edgeKey = genEdgeKey(new QualifiedName(edgeQuerySpec.label), startVertexId, edgeQuerySpec.isBackward());
        List<Edge> adjacencyList = readEdgeList(kvs.get(edgeKey));
        List<Subgraph.Edge> edges = new ArrayList<>();
        for (Edge item : adjacencyList) {
            Subgraph.Edge edge = new Subgraph.Edge(startVertexId, item.nextVertexId, item.edgeProperties);
            edges.add(edge);
        }
        return edges;
    }

    @Override
    public Status writeVertex(QualifiedName label, Object id, Object... keyValues) {
        byte[] value = conf.asByteArray(toProperties(keyValues));
        kvs.put(genVertexKey(label, id), value);
        return Status.OK;
    }

    @Override
    public Status writeEdge(QualifiedName edgeLabel,
                            QualifiedName outVertexLabel, Object outVertexId,
                            QualifiedName inVertexLabel, Object inVertexId,
                            Object... keyValues) {
        Properties edgeProperties = toProperties(keyValues);
        byte[] forwardEdgeKey = genEdgeKey(edgeLabel, outVertexId, false);
        byte[] backwardEdgeKey = genEdgeKey(edgeLabel, inVertexId, true);
        byte[] forwardEdgeValue = kvs.get(forwardEdgeKey);
        byte[] backwardEdgeValue = kvs.get(backwardEdgeKey);
        byte[] forwardAdjacencyList = appendToAdjacencyList(forwardEdgeValue, inVertexId, edgeProperties);
        byte[] backwardAdjacencyList = appendToAdjacencyList(backwardEdgeValue, outVertexId, edgeProperties);
        if (forwardAdjacencyList != null) {
            kvs.put(forwardEdgeKey, forwardAdjacencyList);
        }
        if (backwardAdjacencyList != null) {
            kvs.put(backwardEdgeKey, backwardAdjacencyList);
        }
        return Status.OK;
    }

    protected byte[] appendToAdjacencyList(byte[] existingList, Object vertexId, Properties edgeProperties) {
        List<Edge> adjancencyList = existingList == null ? new ArrayList<>() : (List<Edge>) conf.asObject(existingList);
        Edge edge = new Edge();
        edge.nextVertexId = vertexId;
        edge.edgeProperties = edgeProperties;
        boolean found = false;
        for (Edge e : adjancencyList) {
            if (e.nextVertexId.equals(edge.nextVertexId)) {
                if (e.edgeProperties.equals(edgeProperties)) {
                    return null;
                } else {
                    e.edgeProperties = edge.edgeProperties;
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            adjancencyList.add(edge);
        }
        return conf.asByteArray(adjancencyList);
    }

    private byte[] genVertexKey(QualifiedName label, Object id) {
        return (id + Separator + label).getBytes();
    }

    private byte[] genEdgeKey(QualifiedName edgeLabel, Object startVertexId, boolean isBackward) {
        if (!isBackward) {
            return (startVertexId + Separator + edgeLabel).getBytes();
        }
        return (startVertexId + Separator + edgeLabel + REVERSE_SUFFIX).getBytes();
    }

    protected List<Edge> readEdgeList(byte[] edgeListBytes) {
        if (edgeListBytes == null) {
            return new ArrayList<>();
        }
        return (List<Edge>) conf.asObject(edgeListBytes);
    }

    protected Properties toProperties(Object[] keyValues) {
        Properties properties = new Properties();
        if (keyValues != null) {
            for (int i = 0; i < keyValues.length; i += 2) {
                properties.put(keyValues[i], keyValues[i + 1]);
            }
        }
        return properties;
    }

    public static class Edge implements Serializable {
        public Object nextVertexId;
        public Properties edgeProperties;

        public Edge() {
        }
    }

}
