package com.uber.ugsb.db;

import com.uber.ugsb.queries.QueriesSpec;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.util.Iterator;

public class GremlinDB extends DB implements QueryCapability.SupportGremlin {

    private final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
    private Graph graph;

    public GremlinDB() {
    }

    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    @Override
    public Status writeVertex(String label, Object id, Object... keyValues) {
        Object[] params = new Object[keyValues.length + 4];
        params[0] = T.label;
        params[1] = label;
        params[2] = T.id;
        params[3] = id;
        for (int i = 0; i < keyValues.length; i++) {
            params[i + 4] = keyValues[i];
        }
        graph.addVertex(params);
        return Status.OK;
    }

    @Override
    public Status writeEdge(String edgeLabel,
                            String outVertexLabel, Object outVertexId,
                            String inVertexLabel, Object inVertexId,
                            Object... keyValues) {
        Vertex outVertex = findVertex(outVertexLabel, outVertexId);
        Vertex inVertex = findVertex(inVertexLabel, inVertexId);

        if (outVertex != null && inVertex != null) {
            outVertex.addEdge(edgeLabel, inVertex, keyValues);
            return Status.OK;
        }

        return Status.ERROR;
    }

    private Vertex findVertex(String vertexLabel, Object vertexId) {
        Vertex vertex = null;
        Iterator<Vertex> vertexIterator = graph.vertices(vertexId);
        while (vertexIterator.hasNext()) {
            vertex = vertexIterator.next();
            if (!vertexLabel.equals(vertex.label())) {
                vertex = null;
            } else {
                break;
            }
        }
        return vertex;
    }

    @Override
    public Object queryByGremlin(String gremlinQuery, Object... bindVariables) throws ScriptException {
        Bindings bindings = engine.createBindings();
        bindings.put("g", graph.traversal());
        for (int i = 0; i < bindVariables.length - 1; i += 2) {
            bindings.put(bindVariables[i].toString(), bindVariables[i + 1]);
        }

        return engine.eval(gremlinQuery, bindings);
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

}
