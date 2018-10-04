package com.uber.ugb.db.mock;

import com.uber.ugb.GraphGenerator;
import com.uber.ugb.db.DB;
import com.uber.ugb.db.KeyValueDB;
import com.uber.ugb.db.PrefixKeyValueDB;
import com.uber.ugb.db.Subgraph;
import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.schema.Vocabulary;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static com.uber.ugb.GraphGeneratorTest.newGraphGenerator;
import static org.junit.Assert.assertEquals;

public class GraphDbReadWriteTest {

    public static void testDBReadWrite(DB db) throws IOException {
        initGraphForTest(db);

        QueriesSpec.Query query = new QueriesSpec.Query();
        query.steps = new QueriesSpec.Query.Step[2];
        // step 1
        query.steps[0] = new QueriesSpec.Query.Step();
        query.steps[0].edge = new QueriesSpec.Query.Step.Edge();
        query.steps[0].edge.label = "documents.usedDocument";
        query.steps[0].vertex = new QueriesSpec.Query.Step.Vertex();
        query.steps[0].vertex.select = "documentType";
        // step 2
        query.steps[1] = new QueriesSpec.Query.Step();
        query.steps[1].edge = new QueriesSpec.Query.Step.Edge();
        query.steps[1].edge.label = "documents.usedDocument";
        query.steps[1].edge.direction = "in";
        query.steps[1].edge.select = "status";
        query.steps[1].edge.filter = "status = 'active'";
        query.steps[1].vertex = new QueriesSpec.Query.Step.Vertex();
        query.steps[1].vertex.select = "  name "; // has some extra spaces

        Subgraph subgraph = new Subgraph(1L);
        db.subgraph(query, subgraph);

        Subgraph.Edge subgraphEdge = subgraph.findEdge(1L, 11L);
        assertEquals(true, subgraphEdge != null);
        Properties subgraphEdgeVertexProperties = subgraphEdge.getVertexProperties();
        assertEquals(true, subgraphEdgeVertexProperties != null);
        assertEquals("DRIVER_LICENSE", subgraphEdgeVertexProperties.getProperty("documentType"));
        assertEquals(null, subgraphEdgeVertexProperties.getProperty("documentId"));

        assertEquals(true, subgraph.findEdge(11L, 1L) != null);
        assertEquals("name1", subgraph.findEdge(11L, 1L).getVertexProperties().getProperty("name"));
        assertEquals(true, subgraph.findEdge(11L, 2L) != null);
        assertEquals("name2", subgraph.findEdge(11L, 2L).getVertexProperties().getProperty("name"));
        assertEquals(true, subgraph.findEdge(11L, 3L) != null);
        assertEquals("active", subgraph.findEdge(11L, 3L).edgeProperties.getProperty("status"));
        assertEquals(true, subgraph.findEdge(11L, 4L) == null);
        assertEquals(true, subgraph.findEdge(11L, 5L) != null);
        assertEquals("active", subgraph.findEdge(11L, 5L).edgeProperties.getProperty("status"));

    }

    public static void initGraphForTest(DB db) throws IOException {
        GraphGenerator graphGenerator = newGraphGenerator();
        Vocabulary vocabulary = graphGenerator.getModel().getSchemaVocabulary();
        db.setVocabulary(vocabulary);

        db.writeVertex(new QualifiedName("users.User"), 1L, "name", "name1");
        db.writeVertex(new QualifiedName("users.User"), 2L, "name", "name2");
        db.writeVertex(new QualifiedName("users.User"), 3L, "name", "name3");
        db.writeVertex(new QualifiedName("users.User"), 4L, "name", "name4");
        db.writeVertex(new QualifiedName("users.User"), 5L, "name", "name5");
        db.writeVertex(new QualifiedName("documents.Document"), 11L,
            "documentType", "DRIVER_LICENSE", "documentId", "1234345");
        db.writeVertex(new QualifiedName("documents.Document"), 12L);
        db.writeVertex(new QualifiedName("documents.Document"), 13L);
        db.writeVertex(new QualifiedName("documents.Document"), 14L);
        db.writeVertex(new QualifiedName("documents.Document"), 15L);
        db.writeEdge(new QualifiedName("documents.usedDocument"),
            new QualifiedName("users.User"), 1L, new QualifiedName("documents.Document"), 11L, "status", "active");
        db.writeEdge(new QualifiedName("documents.usedDocument"),
            new QualifiedName("users.User"), 2L, new QualifiedName("documents.Document"), 11L, "status", "active");
        db.writeEdge(new QualifiedName("documents.usedDocument"),
            new QualifiedName("users.User"), 2L, new QualifiedName("documents.Document"), 12L);
        db.writeEdge(new QualifiedName("documents.usedDocument"),
            new QualifiedName("users.User"), 3L, new QualifiedName("documents.Document"), 11L, "status", "active");
        db.writeEdge(new QualifiedName("documents.usedDocument"),
            new QualifiedName("users.User"), 3L, new QualifiedName("documents.Document"), 12L);
        db.writeEdge(new QualifiedName("documents.usedDocument"),
            new QualifiedName("users.User"), 3L, new QualifiedName("documents.Document"), 13L);
        db.writeEdge(new QualifiedName("documents.usedDocument"),
            new QualifiedName("users.User"), 4L, new QualifiedName("documents.Document"), 11L, "status", "inactive");
        db.writeEdge(new QualifiedName("documents.usedDocument"),
            new QualifiedName("users.User"), 5L, new QualifiedName("documents.Document"), 11L, "status", "active");
    }

    @Test
    public void testMockMemDocumentDB() throws IOException {
        testDBReadWrite(new MockMemAbstractSubgraphDB());
    }

    @Test
    public void testKeyValueDB() throws IOException {
        KeyValueDB kvdb = new KeyValueDB();
        kvdb.setKeyValueStore(new MockKeyValueStore());

        testDBReadWrite(kvdb);
    }

    @Test
    public void testPrefixKeyValueDB() throws IOException {
        PrefixKeyValueDB kvdb = new PrefixKeyValueDB();
        kvdb.setPrefixKeyValueStore(new MockPrefixKeyValueStore());

        testDBReadWrite(kvdb);
    }

}
