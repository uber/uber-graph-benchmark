package com.uber.ugb;

import com.uber.ugb.db.GremlinDB;
import com.uber.ugb.model.GraphModel;
import com.uber.ugb.schema.InvalidSchemaException;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.schema.SchemaUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class GraphGeneratorTest extends GraphGenTestBase {

    public static GraphGenerator newGraphGenerator() throws IOException {
        ClassLoader loader = GraphGeneratorTest.class.getClassLoader();
        GraphModelBuilder graphModelBuilder = new GraphModelBuilder();
        graphModelBuilder.addConcept(loader.getResourceAsStream("trips/concepts/core.yaml"));
        graphModelBuilder.addConcept(loader.getResourceAsStream("trips/concepts/devices.yaml"));
        graphModelBuilder.addConcept(loader.getResourceAsStream("trips/concepts/documents.yaml"));
        graphModelBuilder.addConcept(loader.getResourceAsStream("trips/concepts/payments.yaml"));
        graphModelBuilder.addConcept(loader.getResourceAsStream("trips/concepts/referrals.yaml"));
        graphModelBuilder.addConcept(loader.getResourceAsStream("trips/concepts/trips.yaml"));
        graphModelBuilder.addConcept(loader.getResourceAsStream("trips/concepts/users.yaml"));
        graphModelBuilder.addConcept(loader.getResourceAsStream("trips/concepts/vehicles.yaml"));
        graphModelBuilder.setStatisticsInputStream(loader.getResourceAsStream("trips/statistics.yaml"));
        GraphModel model = graphModelBuilder.build();
        GraphGenerator gen = new GraphGenerator(model);
        return gen;
    }

    @Test
    public void vertexLabelFrequenciesMatchInputPartition() throws Exception {
        int totalVertices = 10000;
        Graph graph = SchemaUtils.createTinkerGraph();
        GremlinDB gremlinDB = new GremlinDB();
        gremlinDB.setGraph(graph);
        GraphGenerator gen = newGraphGenerator();
        gen.generateTo(gremlinDB, totalVertices);

        assertTrue(Math.abs(totalVertices - count(graph.traversal().V())) < 5);

        Map<QualifiedName, Float> weights = gen.getModel().getVertexPartitioner().getWeightByLabel();
        float totalWeight = 0;
        Map<QualifiedName, Integer> counts = new HashMap<>();
        for (Map.Entry<QualifiedName, Float> e : weights.entrySet()) {
            QualifiedName label = e.getKey();
            totalWeight += e.getValue();
            counts.put(label, 0);
        }
        forAllVertices(graph, vertex -> {
            QualifiedName label = new QualifiedName(vertex.label());
            counts.put(label, 1 + counts.get(label));
        });
        for (Map.Entry<QualifiedName, Integer> e : counts.entrySet()) {
            QualifiedName label = e.getKey();
            int actualCount = e.getValue();
            double idealCount = totalVertices * weights.get(label) / (1.0 * totalWeight);
            assertEquals(idealCount, actualCount, 1.0);
        }
    }

    @Test
    public void graphEdgesAreNondeterministicIfRandomSeedIsNotSet() throws Exception {
        int totalVertices = 10000;
        long prevCount = 0L;
        for (int i = 0; i < 10; i++) {
            long count = generateGraphAndCountEdges(totalVertices, 0);
            if (i > 0 && count != prevCount) {
                return;
            }
            Thread.sleep(1);
            prevCount = count;
        }
        fail("all graphs have the same number of edges");
    }

    @Test
    public void graphEdgesAreDeterministicIfRandomSeedIsSet() throws Exception {
        int totalVertices = 10000;
        long prevCount = 0L;
        long randomSeed = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            long count = generateGraphAndCountEdges(totalVertices, randomSeed);
            if (i > 0 && count != prevCount) {
                fail("graph edges are nondeterministic");
            }
            Thread.sleep(1);
            prevCount = count;
        }
    }

    /*
    @Test
    public void csvPropertiesAreWrittenToFiles() throws Exception {
        GraphGenerator gen = newGraphGenerator();
        Set<RelationType> csvProps = new HashSet<>();
        //csvProps.add(gen.getModel().getSchemaVocabulary().getRelationTypes().get(
        //        new QualifiedName("core", "uuid")));
        gen.setCsvProps(csvProps);
        File dir = createTempDir();
        gen.setCsvDir(dir);

        int nVertices = 100;
        Graph graph = SchemaUtils.createTinkerGraph();
        gen.generateTo(graph, nVertices);
        File uuidFile = new File(dir, "core.uuid.csv");
        assertTrue(uuidFile.exists());

        Map<String, String> labelById = new HashMap<>();
        Map<String, String> uuidById = new HashMap<>();
        int count = 0;
        try (InputStream in = new FileInputStream(uuidFile)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while (null != (line = reader.readLine())) {
                String[] a = line.split(",");
                assertEquals(3, a.length);
                String id = a[0];
                String label = a[1];
                String value = a[2];
                labelById.put(id, label);
                uuidById.put(id, value);
                count++;
            }
        }
        assertEquals(nVertices, count);

        assertEquals(nVertices, count(graph.traversal().V()));
        Iterator<Vertex> iter = graph.traversal().V();
        while (iter.hasNext()) {
            Vertex v = iter.next();
            assertEquals(v.property("uuid").value().toString(), uuidById.get(v.id().toString()));
            assertEquals(v.label(), labelById.get(v.id().toString()));
        }
    }
    */

    @Ignore
    @Test
    public void verifyStatsManuallyInR() throws Exception {
        int totalVertices = 10000;
        Graph graph = SchemaUtils.createTinkerGraph();
        GremlinDB gremlinDB = new GremlinDB();
        gremlinDB.setGraph(graph);
        GraphGenerator gen = newGraphGenerator();
        gen.generateTo(gremlinDB, totalVertices);

        writeVerticesTo(graph, new File("/tmp/vertices.csv"));
        writeEdgesTo(graph, new File("/tmp/edges.csv"));
        // TODO: vertex properties
    }

    private long generateGraphAndCountEdges(final int totalVertices, final long randomSeed)
        throws IOException, InvalidSchemaException {
        Graph graph = SchemaUtils.createTinkerGraph();
        GremlinDB gremlinDB = new GremlinDB();
        gremlinDB.setGraph(graph);
        GraphGenerator gen = newGraphGenerator();
        if (0 != randomSeed) {
            gen.setRandomSeed(randomSeed);
        }
        gen.generateTo(gremlinDB, totalVertices);
        return countEdges(graph);
    }

    private long countEdges(final Graph graph) {
        return count(graph.traversal().E());
    }

    private void forAllVertices(final Graph graph, final Consumer<Vertex> visitor) {
        Iterator<Vertex> iter = graph.traversal().V();
        while (iter.hasNext()) {
            visitor.accept(iter.next());
        }
    }

    private void writeVerticesTo(final Graph graph, final File file) throws IOException {
        writeTo(file, ps -> {
            ps.println("id,label");
            Iterator<Vertex> iter = graph.traversal().V();
            while (iter.hasNext()) {
                Vertex v = iter.next();
                ps.println(v.id() + "," + v.label());
            }
        });
    }

    private void writeEdgesTo(final Graph graph, final File file) throws IOException {
        writeTo(file, ps -> {
            ps.println("tail,label,head");
            Iterator<Edge> iter = graph.traversal().E();
            while (iter.hasNext()) {
                Edge e = iter.next();
                Object tailId = e.outVertex().id();
                Object headId = e.inVertex().id();
                ps.println(tailId + "," + e.label() + "," + headId);
            }
        });
    }

    private <T> long count(final Iterator<T> iter) {
        long count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        return count;
    }
}
