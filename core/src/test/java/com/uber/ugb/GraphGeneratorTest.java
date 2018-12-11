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

package com.uber.ugb;

import com.uber.ugb.db.DBException;
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
        gen.generateTo(gremlinDB, totalVertices, 2, 1);

        long found = count(graph.traversal().V());

        assertTrue(Math.abs(totalVertices - found) < 5);

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

    @Ignore
    @Test
    public void verifyStatsManuallyInR() throws Exception {
        int totalVertices = 10000;
        Graph graph = SchemaUtils.createTinkerGraph();
        GremlinDB gremlinDB = new GremlinDB();
        gremlinDB.setGraph(graph);
        GraphGenerator gen = newGraphGenerator();
        gen.generateTo(gremlinDB, totalVertices, 4, 2);

        writeVerticesTo(graph, new File("/tmp/vertices.csv"));
        writeEdgesTo(graph, new File("/tmp/edges.csv"));
        // TODO: vertex properties
    }

    private long generateGraphAndCountEdges(final int totalVertices, final long randomSeed)
        throws IOException, InvalidSchemaException, DBException {
        Graph graph = SchemaUtils.createTinkerGraph();
        GremlinDB gremlinDB = new GremlinDB();
        gremlinDB.setGraph(graph);
        GraphGenerator gen = newGraphGenerator();
        if (0 != randomSeed) {
            gen.setRandomSeed(randomSeed);
        }
        gen.generateTo(gremlinDB, totalVertices, 8, 4);
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
