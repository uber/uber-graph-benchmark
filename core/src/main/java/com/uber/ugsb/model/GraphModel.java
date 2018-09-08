package com.uber.ugsb.model;

import com.uber.ugsb.schema.Vocabulary;

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
    private final LinkedHashMap<String, EdgeModel> edgeModels;
    private final LinkedHashMap<String, PropertyModel> vertexPropertyModels;
    private final LinkedHashMap<String, PropertyModel> edgePropertyModels;

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
                      final LinkedHashMap<String, EdgeModel> edgeModels,
                      final LinkedHashMap<String, PropertyModel> vertexPropertyModels,
                      final LinkedHashMap<String, PropertyModel> edgePropertyModels) {
        this.schemaVocabulary = schemaVocabulary;
        this.vertexPartitioner = vertexPartitioner;
        this.edgeModels = edgeModels;
        this.vertexPropertyModels = vertexPropertyModels;
        this.edgePropertyModels = edgePropertyModels;
    }

    public Partitioner getVertexPartitioner() {
        return vertexPartitioner;
    }

    public Map<String, EdgeModel> getEdgeModels() {
        return edgeModels;
    }

    public Map<String, PropertyModel> getVertexPropertyModels() {
        return vertexPropertyModels;
    }

    public Map<String, PropertyModel> getEdgePropertyModels() {
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
