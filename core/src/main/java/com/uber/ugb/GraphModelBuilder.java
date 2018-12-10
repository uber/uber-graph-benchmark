package com.uber.ugb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.uber.ugb.model.EdgeModel;
import com.uber.ugb.model.GraphModel;
import com.uber.ugb.model.Incidence;
import com.uber.ugb.model.Partitioner;
import com.uber.ugb.model.PropertyModel;
import com.uber.ugb.schema.InvalidSchemaException;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.schema.SchemaBuilder;
import com.uber.ugb.schema.Vocabulary;
import com.uber.ugb.schema.model.EntityType;
import com.uber.ugb.schema.model.RelationType;
import com.uber.ugb.statistics.StatisticsSpec;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GraphModelBuilder {

    List<InputStream> conceptInputStreams;
    InputStream statisticsInputStream;
    Map<String, String> csvDistribution;

    public GraphModelBuilder() {
        this.conceptInputStreams = new ArrayList<>();
        this.csvDistribution = new HashMap<>();
    }

    public void addConceptDirectory(File graghConceptDir) throws IOException, InvalidSchemaException {
        if (!graghConceptDir.isDirectory()) {
            throw new InvalidSchemaException("not a directory: " + graghConceptDir);
        }
        for (File file : graghConceptDir.listFiles()) {
            if (file.getName().endsWith(".yaml")) {
                addConcept(new FileInputStream(file));
            } else if (file.isDirectory()) {
                addConceptDirectory(file);
            }
        }
    }

    public void setDistributionDirectory(File edgeDistrbutionDir) throws IOException, InvalidSchemaException {
        if (!edgeDistrbutionDir.isDirectory()) {
            throw new InvalidSchemaException("not a directory: " + edgeDistrbutionDir);
        }
        for (File file : edgeDistrbutionDir.listFiles()) {
            String fileName = file.getName();
            if (fileName.endsWith(".csv")) {
                setEdgeDistribution(fileName.substring(0, fileName.length() - ".csv".length()),
                    new String(Files.readAllBytes(file.toPath()))
                );
            } else if (file.isDirectory()) {
                setDistributionDirectory(file);
            }
        }
    }

    public void addConcept(InputStream concept) {
        this.conceptInputStreams.add(concept);
    }

    public void setStatistics(File statisticsFile) throws FileNotFoundException {
        this.statisticsInputStream = new FileInputStream(statisticsFile);
    }

    public void setStatisticsInputStream(InputStream statisticsInputStream) {
        this.statisticsInputStream = statisticsInputStream;
    }

    public void setEdgeDistribution(String edgeLabel, String csvContent) {
        this.csvDistribution.put(edgeLabel, csvContent);
    }

    public GraphModel build() throws IOException, InvalidSchemaException {

        Vocabulary vocabulary = buildVocabulary();

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        StatisticsSpec statisticsSpec = objectMapper.readValue(this.statisticsInputStream, StatisticsSpec.class);

        Partitioner vertexPartitioner = buildVertexPartitioner(statisticsSpec);
        LinkedHashMap<QualifiedName, EdgeModel> edgeModel = buildEdgeModel(vocabulary, statisticsSpec);

        LinkedHashMap<QualifiedName, PropertyModel> vertexPropertyModel =
            buildVertexPropertyModel(vocabulary, statisticsSpec);
        LinkedHashMap<QualifiedName, PropertyModel> edgePropertyModel =
            buildEdgePropertyModel(vocabulary, statisticsSpec);

        return new GraphModel(vocabulary, vertexPartitioner, edgeModel, vertexPropertyModel, edgePropertyModel);


    }

    private Vocabulary buildVocabulary() throws IOException {
        SchemaBuilder sb = new SchemaBuilder();
        for (InputStream inputStream : this.conceptInputStreams) {
            sb.addSchema(inputStream);
        }
        return sb.toVocabulary();
    }

    private Partitioner buildVertexPartitioner(StatisticsSpec statisticsSpec) {
        Partitioner vertexPartitioner = new Partitioner();
        for (StatisticsSpec.VertexWeight vertexWeight : statisticsSpec.vertices) {
            vertexPartitioner.put(new QualifiedName(vertexWeight.type), vertexWeight.weight);
        }
        return vertexPartitioner;
    }

    private LinkedHashMap<QualifiedName, EdgeModel> buildEdgeModel(Vocabulary vocabulary, StatisticsSpec statisticsSpec) {
        LinkedHashMap<QualifiedName, EdgeModel> edgeModel = new LinkedHashMap<>();
        for (StatisticsSpec.EdgeDistribution edgeDistribution : statisticsSpec.edges) {
            RelationType relationType = vocabulary.getRelationType(new QualifiedName(edgeDistribution.type));
            String csvContnet = this.csvDistribution.get(edgeDistribution.type);
            if (csvContnet == null) {
                edgeModel.put(new QualifiedName(edgeDistribution.type),
                    new EdgeModel(
                        new Incidence(
                            relationType.getFrom().getName(),
                            edgeDistribution.out.existenceProbability,
                            edgeDistribution.out.toDegreeDistribution()),
                        new Incidence(
                            relationType.getTo().getName(),
                            edgeDistribution.in.existenceProbability,
                            edgeDistribution.in.toDegreeDistribution())
                    )
                );
            } else {
                edgeModel.put(new QualifiedName(edgeDistribution.type),
                    new EdgeModel(
                        new Incidence(
                            relationType.getFrom().getName(),
                            edgeDistribution.out.existenceProbability,
                            csvContnet, "out", edgeDistribution.type),
                        new Incidence(
                            relationType.getTo().getName(),
                            edgeDistribution.in.existenceProbability,
                            csvContnet, "in", edgeDistribution.type)
                    )
                );
            }
        }
        return edgeModel;
    }

    private LinkedHashMap<QualifiedName, PropertyModel> buildVertexPropertyModel(
        Vocabulary vocabulary, StatisticsSpec statisticsSpec) {

        Map<QualifiedName, StatisticsSpec.PropertyValueWeight[]> customPropertyModels = new HashMap<>();
        for (StatisticsSpec.PropertyValues propertyValues : statisticsSpec.properties) {
            customPropertyModels.put(new QualifiedName(propertyValues.type), propertyValues.propertyValueWeights);
        }

        LinkedHashMap<QualifiedName, PropertyModel> vertexPropertyStats = new LinkedHashMap<>();
        for (StatisticsSpec.VertexWeight vertexWeight : statisticsSpec.vertices) {
            EntityType entityType = vocabulary.getEntityType(new QualifiedName(vertexWeight.type));

            PropertyModel propStats = new PropertyModel(vocabulary, entityType, customPropertyModels);
            vertexPropertyStats.put(new QualifiedName(vertexWeight.type), propStats);
        }
        return vertexPropertyStats;
    }

    private LinkedHashMap<QualifiedName, PropertyModel> buildEdgePropertyModel(
        Vocabulary vocabulary, StatisticsSpec statisticsSpec) {

        Map<QualifiedName, StatisticsSpec.PropertyValueWeight[]> customPropertyModels = new HashMap<>();
        for (StatisticsSpec.PropertyValues propertyValues : statisticsSpec.properties) {
            customPropertyModels.put(new QualifiedName(propertyValues.type), propertyValues.propertyValueWeights);
        }

        LinkedHashMap<QualifiedName, PropertyModel> edgePropertyStats = new LinkedHashMap<>();
        for (StatisticsSpec.EdgeDistribution edgeDistribution : statisticsSpec.edges) {
            RelationType entityType = vocabulary.getRelationType(new QualifiedName(edgeDistribution.type));

            PropertyModel propStats = new PropertyModel(vocabulary, entityType, customPropertyModels);
            edgePropertyStats.put(new QualifiedName(edgeDistribution.type), propStats);
        }
        return edgePropertyStats;
    }

}
