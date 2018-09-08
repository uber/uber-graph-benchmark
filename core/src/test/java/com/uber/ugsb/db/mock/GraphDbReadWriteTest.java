package com.uber.ugsb.db.mock;

import com.uber.ugsb.db.DB;
import com.uber.ugsb.db.KeyValueDB;
import com.uber.ugsb.db.PrefixKeyValueDB;
import com.uber.ugsb.db.Subgraph;
import com.uber.ugsb.queries.QueriesSpec;
import com.uber.ugsb.schema.UgraphVocabulary;
import com.uber.ugsb.schema.Vocabulary;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class GraphDbReadWriteTest {

    public static void testDBReadWrite(DB db) {
        initGraphForTest(db);

        QueriesSpec.Query query = new QueriesSpec.Query();
        query.steps = new QueriesSpec.Query.Step[2];
        // step 1
        query.steps[0] = new QueriesSpec.Query.Step();
        query.steps[0].edge = new QueriesSpec.Query.Step.Edge();
        query.steps[0].edge.label = "usedDocument";
        query.steps[0].vertex = new QueriesSpec.Query.Step.Vertex();
        query.steps[0].vertex.select = "documentType";
        // step 2
        query.steps[1] = new QueriesSpec.Query.Step();
        query.steps[1].edge = new QueriesSpec.Query.Step.Edge();
        query.steps[1].edge.label = "usedDocument";
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

    public static void initGraphForTest(DB db) {
        Vocabulary vocabulary = UgraphVocabulary.getInstance();
        db.setVocabulary(vocabulary);

        db.writeVertex("User", 1L, "name", "name1");
        db.writeVertex("User", 2L, "name", "name2");
        db.writeVertex("User", 3L, "name", "name3");
        db.writeVertex("User", 4L, "name", "name4");
        db.writeVertex("User", 5L, "name", "name5");
        db.writeVertex("Document", 11L, "documentType", "DRIVER_LICENSE", "documentId", "1234345");
        db.writeVertex("Document", 12L);
        db.writeVertex("Document", 13L);
        db.writeVertex("Document", 14L);
        db.writeVertex("Document", 15L);
        db.writeEdge("usedDocument", "User", 1L, "Document", 11L, "status", "active");
        db.writeEdge("usedDocument", "User", 2L, "Document", 11L, "status", "active");
        db.writeEdge("usedDocument", "User", 2L, "Document", 12L);
        db.writeEdge("usedDocument", "User", 3L, "Document", 11L, "status", "active");
        db.writeEdge("usedDocument", "User", 3L, "Document", 12L);
        db.writeEdge("usedDocument", "User", 3L, "Document", 13L);
        db.writeEdge("usedDocument", "User", 4L, "Document", 11L, "status", "inactive");
        db.writeEdge("usedDocument", "User", 5L, "Document", 11L, "status", "active");
    }

    @Test
    public void testMockMemDocumentDB() {
        testDBReadWrite(new MockMemAbstractSubgraphDB());
    }

    @Test
    public void testKeyValueDB() {
        KeyValueDB kvdb = new KeyValueDB();
        kvdb.setKeyValueStore(new MockKeyValueStore());

        testDBReadWrite(kvdb);
    }

    @Test
    public void testPrefixKeyValueDB() {
        PrefixKeyValueDB kvdb = new PrefixKeyValueDB();
        kvdb.setPrefixKeyValueStore(new MockPrefixKeyValueStore());

        testDBReadWrite(kvdb);
    }

}
