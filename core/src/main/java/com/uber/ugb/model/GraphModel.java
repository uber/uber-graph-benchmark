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

package com.uber.ugb.model;

import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.schema.Vocabulary;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class GraphModel implements Serializable {

    private static final long serialVersionUID = Vocabulary.serialVersionUID;

    private final Vocabulary schemaVocabulary;
    private final Partitioner vertexPartitioner;
    private final LinkedHashMap<QualifiedName, EdgeModel> edgeModels;
    private final LinkedHashMap<QualifiedName, PropertyModel> vertexPropertyModels;
    private final LinkedHashMap<QualifiedName, PropertyModel> edgePropertyModels;

    /**
     * Constructs a new graph model
     *
     * @param schemaVocabulary     the schema of the graph model
     * @param vertexPartitioner    a partitioner for the vertex label distribution of the modeled graph
     * @param edgeModels           an edge model for each edge label.
     *                             LinkedHashMap is used in order to ensure consistent serialization.
     * @param vertexPropertyModels a property model for each vertex label
     *                             LinkedHashMap is used in order to ensure consistent serialization.
     * @param edgePropertyModels   a property model for each edge label
     *                             LinkedHashMap is used in order to ensure consistent serialization.
     */
    public GraphModel(final Vocabulary schemaVocabulary,
                      final Partitioner vertexPartitioner,
                      final LinkedHashMap<QualifiedName, EdgeModel> edgeModels,
                      final LinkedHashMap<QualifiedName, PropertyModel> vertexPropertyModels,
                      final LinkedHashMap<QualifiedName, PropertyModel> edgePropertyModels) {
        this.schemaVocabulary = schemaVocabulary;
        this.vertexPartitioner = vertexPartitioner;
        this.edgeModels = edgeModels;
        this.vertexPropertyModels = vertexPropertyModels;
        this.edgePropertyModels = edgePropertyModels;
    }

    public Partitioner getVertexPartitioner() {
        return vertexPartitioner;
    }

    public Map<QualifiedName, EdgeModel> getEdgeModels() {
        return edgeModels;
    }

    public Map<QualifiedName, PropertyModel> getVertexPropertyModels() {
        return vertexPropertyModels;
    }

    public Map<QualifiedName, PropertyModel> getEdgePropertyModels() {
        return edgePropertyModels;
    }

    public Vocabulary getSchemaVocabulary() {
        return schemaVocabulary;
    }

    public String getHash() {
        int hashCode;
        try {
            hashCode = Arrays.hashCode(toByteArray());
        } catch (IOException e) {
            // a ByteArrayOutputStream won't throw IOException here, regardless of input
            throw new IllegalStateException(e);
        }
        return Integer.toString(Math.abs(hashCode), 16);
    }

    private byte[] toByteArray() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(this);
            out.flush();
            return bos.toByteArray();
        }
    }
}
