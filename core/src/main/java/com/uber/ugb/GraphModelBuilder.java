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
import com.uber.ugb.schema.model.RelationType;
import com.uber.ugb.statistics.StatisticsSpec;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class GraphModelBuilder {

    List<InputStream> conceptInputStreams;
    InputStream statisticsInputStream;

    GraphModelBuilder() {
        this.conceptInputStreams = new ArrayList<>();
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

    public void addConcept(InputStream concept) {
        this.conceptInputStreams.add(concept);
    }

    public void setStatistics(File statisticsFile) throws FileNotFoundException {
        this.statisticsInputStream = new FileInputStream(statisticsFile);
    }

    public void setStatisticsInputStream(InputStream statisticsInputStream) {
        this.statisticsInputStream = statisticsInputStream;
    }

    public GraphModel build() throws IOException, InvalidSchemaException {

        Vocabulary vocabulary = buildVocabulary();

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        StatisticsSpec statisticsSpec = objectMapper.readValue(this.statisticsInputStream, StatisticsSpec.class);

        Partitioner vertexPartitioner = buildVertexPartitioner(statisticsSpec);
        LinkedHashMap<String, EdgeModel> edgeModel = buildEdgeModel(vocabulary, statisticsSpec);

        LinkedHashMap<String, PropertyModel> vertexPropertyModel
            = buildVertexPropertyModel(vertexPartitioner.getLabels());
        LinkedHashMap<String, PropertyModel> edgePropertyModel = new LinkedHashMap<>();

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
            vertexPartitioner.put(vertexWeight.type, vertexWeight.weight);
        }
        return vertexPartitioner;
    }

    private LinkedHashMap<String, EdgeModel> buildEdgeModel(Vocabulary vocabulary, StatisticsSpec statisticsSpec) {
        LinkedHashMap<String, EdgeModel> edgeModel = new LinkedHashMap<>();
        for (StatisticsSpec.EdgeDistribution edgeDistribution : statisticsSpec.edges) {
            RelationType relationType = vocabulary.getRelationType(new QualifiedName(edgeDistribution.type));
            edgeModel.put(edgeDistribution.type,
                new EdgeModel(
                    new Incidence(
                        relationType.getFrom().getLabel(),
                        edgeDistribution.out.existenceProbability,
                        edgeDistribution.out.toDegreeDistribution()),
                    new Incidence(
                        relationType.getTo().getLabel(),
                        edgeDistribution.in.existenceProbability,
                        edgeDistribution.in.toDegreeDistribution())
                )
            );
        }
        return edgeModel;
    }

    private LinkedHashMap<String, PropertyModel> buildVertexPropertyModel(Set<String> vertexLabels) {
        PropertyModel propStats = new PropertyModel();
        LinkedHashMap<String, PropertyModel> vertexPropertyStats = new LinkedHashMap<>();
        for (String vertexLabel : vertexLabels) {
            vertexPropertyStats.put(vertexLabel, propStats);
        }
        return vertexPropertyStats;
    }

}
