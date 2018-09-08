package com.uber.ugsb.db;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.uber.ugsb.queries.QueriesSpec;
import com.uber.ugsb.storage.PrefixKeyValueStore;
import org.nustaq.serialization.FSTConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class PrefixKeyValueDB extends AbstractSubgraphDB {

    private static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
    private static byte Separator = 0x01;
    private static byte REVERSE_SUFFIX = 'r';

    static {
        conf.registerClass(Properties.class);
    }

    private PrefixKeyValueStore kvs;

    public PrefixKeyValueDB() {
    }

    public void setPrefixKeyValueStore(PrefixKeyValueStore kvs) {
        this.kvs = kvs;
    }

    @Override
    public Properties readVertex(String label, Object id, QueriesSpec.Query.Step.Vertex vertexQuerySpec) {
        byte[] value = kvs.get(genVertexKey(label, id));
        if (value == null) {
            return new Properties();
        }
        return (Properties) conf.asObject(value);
    }

    @Override
    public List<Subgraph.Edge> readEdges(Object startVertexId, QueriesSpec.Query.Step.Edge edgeQuerySpec) {
        byte[] prefix = genEdgeKeyPrefix(edgeQuerySpec.label, startVertexId, edgeQuerySpec.isBackward());

        List<Subgraph.Edge> edges = new ArrayList<>();
        List<PrefixKeyValueStore.Row> rows = kvs.scan(prefix, edgeQuerySpec.limit);
        for (PrefixKeyValueStore.Row row : rows) {
            Object nextVertexId = conf.asObject(Arrays.copyOfRange(row.key, prefix.length, row.key.length));
            Properties edgeProperties = (Properties) conf.asObject(row.value);
            Subgraph.Edge edge = new Subgraph.Edge(startVertexId, nextVertexId, edgeProperties);
            edges.add(edge);
        }
        return edges;
    }

    @Override
    public Status writeVertex(String label, Object id, Object... keyValues) {
        byte[] value = propertiesToBytes(keyValues);
        kvs.put(genVertexKey(label, id), value);
        return Status.OK;
    }

    @Override
    public Status writeEdge(String edgeLabel,
                            String outVertexLabel, Object outVertexId,
                            String inVertexLabel, Object inVertexId,
                            Object... keyValues) {
        byte[] edgePropertiesValue = propertiesToBytes(keyValues);
        kvs.put(genEdgeKey(edgeLabel, outVertexId, inVertexId, false), edgePropertiesValue);
        kvs.put(genEdgeKey(edgeLabel, outVertexId, inVertexId, true), edgePropertiesValue);
        return Status.OK;
    }

    protected byte[] genVertexKey(String label, Object id) {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.write(label.getBytes());
        out.write(Separator);
        out.write(id.toString().getBytes());
        return out.toByteArray();
    }

    protected byte[] genEdgeKey(String edgeLabel, Object outVertexId, Object inVertexId, boolean isBackward) {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.write(edgeLabel.getBytes());
        if (isBackward) {
            out.write(REVERSE_SUFFIX);
        }
        out.write(Separator);
        if (isBackward) {
            out.write(inVertexId.toString().getBytes());
        } else {
            out.write(outVertexId.toString().getBytes());
        }
        out.write(Separator);
        if (isBackward) {
            out.write(conf.asByteArray(outVertexId));
        } else {
            out.write(conf.asByteArray(inVertexId));
        }
        return out.toByteArray();
    }

    protected byte[] genEdgeKeyPrefix(String edgeLabel, Object startVertexId, boolean isBackward) {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.write(edgeLabel.getBytes());
        if (isBackward) {
            out.write(REVERSE_SUFFIX);
        }
        out.write(Separator);
        out.write(startVertexId.toString().getBytes());
        out.write(Separator);
        return out.toByteArray();
    }

    private byte[] propertiesToBytes(Object[] keyValues) {
        Properties properties = new Properties();
        if (keyValues != null) {
            for (int i = 0; i < keyValues.length; i += 2) {
                properties.put(keyValues[i], keyValues[i + 1]);
            }
        }
        return conf.asByteArray(properties);
    }

}
