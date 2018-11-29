package com.uber.ugb.db;

import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.util.Iterator;

public class GremlinDB extends DB {

    private final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
    private Graph graph;

    public GremlinDB() {
    }

    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    @Override
    public Status writeVertex(QualifiedName label, Object id, Object... keyValues) {
        Object[] params = new Object[keyValues.length + 4];
        params[0] = T.label;
        params[1] = label.toString();
        params[2] = T.id;
        params[3] = id;
        for (int i = 0; i < keyValues.length; i++) {
            params[i + 4] = keyValues[i];
        }
        graph.addVertex(params);
        return Status.OK;
    }

    @Override
    public Status writeEdge(QualifiedName edgeLabel,
                            QualifiedName outVertexLabel, Object outVertexId,
                            QualifiedName inVertexLabel, Object inVertexId,
                            Object... keyValues) {
        Vertex outVertex = findVertex(outVertexLabel, outVertexId);
        Vertex inVertex = findVertex(inVertexLabel, inVertexId);

        if (outVertex != null && inVertex != null) {
            outVertex.addEdge(edgeLabel.toString(), inVertex, keyValues);
            return Status.OK;
        }

        return Status.ERROR;
    }

    private Vertex findVertex(QualifiedName vertexLabel, Object vertexId) {
        Vertex vertex = null;
        Iterator<Vertex> vertexIterator = graph.vertices(vertexId);
        while (vertexIterator.hasNext()) {
            vertex = vertexIterator.next();
            if (!vertexLabel.toString().equals(vertex.label())) {
                vertex = null;
            } else {
                break;
            }
        }
        return vertex;
    }

    @Override
    public Status subgraph(QueriesSpec.Query query, Subgraph subgraph) {

        return Status.NOT_IMPLEMENTED;

    }

    @Override
    public Status commitBatch() {
        if (graph.features().graph().supportsTransactions()) {
            graph.tx().commit();
        }
        return Status.OK;
    }

    @Override
    public String supportedQueryType() {
        return "gremlin";
    }

    @Override
    public QueryResult executeQuery(String query, Object startVertexId) {
        try {
            TinkerGraph g2 = (TinkerGraph) this.queryByGremlin(query, "x", startVertexId);
            int vertexCount = g2.traversal().V().count().next().intValue();
            int edgeCount = g2.traversal().E().count().next().intValue();
            return new QueryResult(vertexCount, edgeCount);
        } catch (ScriptException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Object queryByGremlin(String gremlinQuery, Object... bindVariables) throws ScriptException {
        Bindings bindings = engine.createBindings();
        bindings.put("g", graph.traversal());
        for (int i = 0; i < bindVariables.length - 1; i += 2) {
            bindings.put(bindVariables[i].toString(), bindVariables[i + 1]);
        }

        return engine.eval(gremlinQuery, bindings);
    }

}
