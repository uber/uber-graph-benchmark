package com.uber.ugb;

import com.uber.ugb.model.EdgeModel;
import com.uber.ugb.model.GraphModel;
import com.uber.ugb.model.Incidence;
import com.uber.ugb.model.Partitioner;
import com.uber.ugb.model.PropertyModel;
import com.uber.ugb.model.distro.ConstantDegreeDistribution;
import com.uber.ugb.model.distro.LogNormalDegreeDistribution;
import com.uber.ugb.schema.InvalidSchemaException;
import com.uber.ugb.schema.UgraphVocabulary;
import com.uber.ugb.schema.Vocabulary;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Set;

public class UgraphGenerator extends GraphGenerator {

    public UgraphGenerator() throws IOException, InvalidSchemaException {
        super(createModel());
    }

    private static Vocabulary createVocabulary() throws IOException, InvalidSchemaException {
        return UgraphVocabulary.getInstance();
    }

    private static GraphModel createModel() throws IOException, InvalidSchemaException {
        Vocabulary vocabulary = createVocabulary();

        Partitioner vertexPartitioner = createVertexPartitioner();

        LinkedHashMap<String, EdgeModel> edgeModel = createEdgeModel();

        LinkedHashMap<String, PropertyModel> vertexPropertyModel
                = createVertexPropertyModel(vertexPartitioner.getLabels(), vocabulary);
        LinkedHashMap<String, PropertyModel> edgePropertyModel = new LinkedHashMap<>();

        return new GraphModel(vocabulary, vertexPartitioner, edgeModel, vertexPropertyModel, edgePropertyModel);
    }

    private static Partitioner createVertexPartitioner() {
        Partitioner vertexPartitioner = new Partitioner();
        vertexPartitioner.put(UgraphVocabulary.VertexType.USER.toString(), 13);
        vertexPartitioner.put(UgraphVocabulary.VertexType.TRIP.toString(), 8);
        vertexPartitioner.put(UgraphVocabulary.VertexType.DOCUMENT.toString(), 1);
        return vertexPartitioner;
    }

    private static LinkedHashMap<String, EdgeModel> createEdgeModel() {
        LinkedHashMap<String, EdgeModel> edgeModel = new LinkedHashMap<>();
        edgeModel.put(UgraphVocabulary.EdgeType.DROVE_FOR.getType().getLabel(), edge(
                // TODO: only a first approximation; does not fit well for low degree
                domain(UgraphVocabulary.VertexType.USER.getType().getLabel(),
                        0.0580, 2.6651985, 0.2999116),
                domain(UgraphVocabulary.VertexType.TRIP.getType().getLabel(),
                        0.9365)));
        edgeModel.put(UgraphVocabulary.EdgeType.USED_DOCUMENT.getType().getLabel(), edge(
                // note: ignoring the 0.2% minority of users with documents who have more than one
                domain(UgraphVocabulary.VertexType.USER.getType().getLabel(),
                        0.2315),
                // note: the distribution appears to be too extreme to model accurately using the available graphgen
                domain(UgraphVocabulary.VertexType.DOCUMENT.getType().getLabel(),
                        1.0000, -636.5463, 17.5147)));
        edgeModel.put(UgraphVocabulary.EdgeType.REQUESTED.getType().getLabel(), edge(
                // TODO: supernode distro
                domain(UgraphVocabulary.VertexType.USER.getType().getLabel(),
                        0.0304, 0.4645424, 0.6449740),
                domain(UgraphVocabulary.VertexType.TRIP.getType().getLabel(),
                        0.1085)));
        return edgeModel;
    }

    private static LinkedHashMap<String, PropertyModel> createVertexPropertyModel(
            final Set<String> vertexLabels, final Vocabulary schemaVocabulary) {
        /*
        RelationType uuid = schemaVocabulary.getRelationTypes().get(new QualifiedName("core", "uuid"));
        SimpleProperty<UUID> uuidProp = new SimpleProperty<>(uuid, random -> {
            // generate a deterministically pseudorandom UUID
            byte[] bytes = new byte[16];
            random.nextBytes(bytes);
            return UUID.nameUUIDFromBytes(bytes);
        });
        */
        PropertyModel propStats = new PropertyModel();
        //propStats.addProperty(uuidProp);

        LinkedHashMap<String, PropertyModel> vertexPropertyStats = new LinkedHashMap<>();
        for (String vertexLabel : vertexLabels) {
            vertexPropertyStats.put(vertexLabel, propStats);
        }
        return vertexPropertyStats;
    }

    private static EdgeModel edge(final Incidence domain,
                                  final Incidence range) {
        return new EdgeModel(domain, range);
    }

    private static Incidence domain(final String vertexLabel,
                                    final double existenceProbability) {
        return new Incidence(
                vertexLabel, existenceProbability, new ConstantDegreeDistribution(1));
    }

    private static Incidence domain(final String vertexLabel,
                                    final double existenceProbability,
                                    final double logMean,
                                    final double logSD) {
        return new Incidence(
                vertexLabel, existenceProbability, new LogNormalDegreeDistribution(logMean, logSD));
    }
}
