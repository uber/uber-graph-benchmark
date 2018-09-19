package com.uber.ugb.model;

import com.uber.ugb.model.distro.ConstantDegreeDistribution;
import com.uber.ugb.model.distro.LogNormalDegreeDistribution;
import com.uber.ugb.schema.InvalidSchemaException;
import com.uber.ugb.schema.SchemaBuilder;
import com.uber.ugb.schema.Vocabulary;
import com.uber.ugb.schema.model.dto.EntityTypeDTO;
import com.uber.ugb.schema.model.dto.RelationTypeDTO;
import com.uber.ugb.schema.model.dto.SchemaDTO;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class GraphModelTest {
    private static Vocabulary createVocabulary() throws InvalidSchemaException, IOException {
        SchemaDTO schemaDTO = new SchemaDTO("test");
        schemaDTO.setIncludes(new String[]{Vocabulary.CORE_SCHEMA_NAME});

        EntityTypeDTO monkey = new EntityTypeDTO("Monkey");
        monkey.setExtends(new String[]{"core.Thing"});
        EntityTypeDTO weasel = new EntityTypeDTO("Weasel");
        EntityTypeDTO mulberryBush = new EntityTypeDTO("MulberryBush");
        weasel.setExtends(new String[]{"core.Thing"});
        schemaDTO.setEntities(new EntityTypeDTO[]{monkey, weasel, mulberryBush});

        RelationTypeDTO chased = new RelationTypeDTO("chased");
        chased.setFrom(monkey.getLabel());
        chased.setTo(weasel.getLabel());
        RelationTypeDTO popped = new RelationTypeDTO("popped");
        popped.setFrom(weasel.getLabel());
        popped.setTo(monkey.getLabel());
        schemaDTO.setRelations(new RelationTypeDTO[]{chased, popped});

        SchemaBuilder builder = new SchemaBuilder();
        builder.addSchema(GraphModelTest.class.getClassLoader().getResourceAsStream("trips/concepts/core.yaml"));
        builder.addSchemas(schemaDTO);

        return builder.toVocabulary();
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

    @Test
    public void modelHashesAreUnique() throws Exception {
        Vocabulary vocabulary = createVocabulary();

        Partitioner vertexPartitioner = new Partitioner();
        vertexPartitioner.put("Monkey", 1);
        vertexPartitioner.put("Weasel", 1);

        LinkedHashMap<String, EdgeModel> edgeModel = new LinkedHashMap<>();
        edgeModel.put("chased", edge(
            domain("Monkey",
                0.9543, 0.7813873, 1.0293729),
            domain("Weasel",
                1.0000)));
        LinkedHashMap<String, PropertyModel> vertexPropertyModels = new LinkedHashMap<>();
        LinkedHashMap<String, PropertyModel> edgePropertyModels = new LinkedHashMap<>();

        GraphModel model = new GraphModel(
            vocabulary, vertexPartitioner, edgeModel, vertexPropertyModels, edgePropertyModels);

        String firstHash = model.getHash();
        String secondHash = model.getHash();
        assertEquals(firstHash, secondHash);

        model = new GraphModel(vocabulary, vertexPartitioner, edgeModel, vertexPropertyModels, edgePropertyModels);
        secondHash = model.getHash();
        assertEquals(firstHash, secondHash);

        vertexPartitioner.put("MulberryBush", 5);
        model = new GraphModel(vocabulary, vertexPartitioner, edgeModel, vertexPropertyModels, edgePropertyModels);
        secondHash = model.getHash();
        assertNotEquals(firstHash, secondHash);

        vertexPartitioner.put("MulberryBush", 5);
        model = new GraphModel(vocabulary, vertexPartitioner, edgeModel, vertexPropertyModels, edgePropertyModels);
        String thirdHash = model.getHash();
        assertEquals(secondHash, thirdHash);

        vertexPartitioner.put("MulberryBush", 7);
        model = new GraphModel(vocabulary, vertexPartitioner, edgeModel, vertexPropertyModels, edgePropertyModels);
        thirdHash = model.getHash();
        assertNotEquals(secondHash, thirdHash);

        /*
        RelationType uuid = vocabulary.getRelationTypes().get(new QualifiedName("core", "uuid"));
        SimpleProperty<String> prop = new SimpleProperty<>(uuid, random -> "pop!");
        PropertyModel propStats = new PropertyModel();
        propStats.addProperty(prop);
        vertexPropertyModels.put("Weasel", propStats);
        model = new GraphModel(vocabulary, vertexPartitioner, edgeModel, vertexPropertyModels, edgePropertyModels);
        String fourthHash = model.getHash();
        assertNotEquals(thirdHash, fourthHash);
        */

        edgeModel = new LinkedHashMap<>();
        edgeModel.put("chased", edge(
            domain("Monkey",
                0.9543, 0.7813873, 1.0293729),
            domain("Weasel",
                0.5))); // change probability
        model = new GraphModel(vocabulary, vertexPartitioner, edgeModel, vertexPropertyModels, edgePropertyModels);
        String fifthHash = model.getHash();
        assertNotEquals(thirdHash, fifthHash);

        edgeModel = new LinkedHashMap<>();
        edgeModel.put("chased", edge(
            domain("Monkey",
                0.9543, 0.5, 1.5), // change log-normal params
            domain("Weasel",
                0.5)));
        model = new GraphModel(vocabulary, vertexPartitioner, edgeModel, vertexPropertyModels, edgePropertyModels);
        String sixthHash = model.getHash();
        assertNotEquals(fifthHash, sixthHash);

        edgeModel.put("popped", edge(
            domain("Weasel",
                0.5),
            domain("Monkey",
                0.5)));
        model = new GraphModel(vocabulary, vertexPartitioner, edgeModel, vertexPropertyModels, edgePropertyModels);
        String seventhHash = model.getHash();
        assertNotEquals(sixthHash, seventhHash);
    }
}
