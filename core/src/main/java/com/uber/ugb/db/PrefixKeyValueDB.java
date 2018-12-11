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

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.storage.PrefixKeyValueStore;
import org.nustaq.serialization.FSTConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PrefixKeyValueDB extends AbstractSubgraphDB {

    private static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
    private static byte Separator = 0x01;
    private static byte REVERSE_SUFFIX = 'r';

    static {
        conf.registerClass(Properties.class);
    }

    private transient PrefixKeyValueStore kvs;

    public PrefixKeyValueDB() {
    }

    public void setPrefixKeyValueStore(PrefixKeyValueStore kvs) {
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
        byte[] prefix = genEdgeKeyPrefix(
            new QualifiedName(edgeQuerySpec.label), startVertexId, edgeQuerySpec.isBackward());

        List<Subgraph.Edge> edges = new ArrayList<>();
        List<PrefixKeyValueStore.PrefixQueriedRow> prefixQueriedRows = kvs.scan(prefix, edgeQuerySpec.limit);
        for (PrefixKeyValueStore.PrefixQueriedRow prefixQueriedRow : prefixQueriedRows) {
            Object nextVertexId = conf.asObject(prefixQueriedRow.keySuffix);
            Properties edgeProperties = (Properties) conf.asObject(prefixQueriedRow.value);
            Subgraph.Edge edge = new Subgraph.Edge(startVertexId, nextVertexId, edgeProperties);
            edges.add(edge);
        }
        return edges;
    }

    @Override
    public Status writeVertex(QualifiedName label, Object id, Object... keyValues) {
        byte[] value = propertiesToBytes(keyValues);
        kvs.put(genVertexKey(label, id), value);
        return Status.OK;
    }

    @Override
    public Status writeEdge(QualifiedName edgeLabel,
                            QualifiedName outVertexLabel, Object outVertexId,
                            QualifiedName inVertexLabel, Object inVertexId,
                            Object... keyValues) {
        byte[] edgePropertiesValue = propertiesToBytes(keyValues);
        kvs.put(genEdgeKeyPrefix(edgeLabel, outVertexId, inVertexId, false),
            genEdgeKeySuffix(outVertexId, inVertexId, false),
            edgePropertiesValue);
        kvs.put(genEdgeKeyPrefix(edgeLabel, outVertexId, inVertexId, true),
            genEdgeKeySuffix(outVertexId, inVertexId, true),
            edgePropertiesValue);
        return Status.OK;
    }

    protected byte[] genVertexKey(QualifiedName label, Object id) {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.write(id.toString().getBytes());
        out.write(Separator);
        out.write(label.toString().getBytes());
        return out.toByteArray();
    }

    protected byte[] genEdgeKeyPrefix(QualifiedName edgeLabel,
                                      Object outVertexId, Object inVertexId, boolean isBackward) {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        if (isBackward) {
            out.write(inVertexId.toString().getBytes());
        } else {
            out.write(outVertexId.toString().getBytes());
        }
        out.write(Separator);
        out.write(edgeLabel.toString().getBytes());
        if (isBackward) {
            out.write(REVERSE_SUFFIX);
        }
        return out.toByteArray();
    }

    protected byte[] genEdgeKeyPrefix(QualifiedName edgeLabel, Object startVertexId, boolean isBackward) {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.write(startVertexId.toString().getBytes());
        out.write(Separator);
        out.write(edgeLabel.toString().getBytes());
        if (isBackward) {
            out.write(REVERSE_SUFFIX);
        }
        return out.toByteArray();
    }

    protected byte[] genEdgeKeySuffix(Object outVertexId, Object inVertexId, boolean isBackward) {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        if (isBackward) {
            out.write(conf.asByteArray(outVertexId));
        } else {
            out.write(conf.asByteArray(inVertexId));
        }
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
