package com.uber.ugb.db;

import com.uber.ugb.queries.QueriesSpec;

/*
 * NoopDB does nothing and only returns Status.OK for every operations.
 */
public class NoopDB extends DB {

    @Override
    public Status writeVertex(String label, Object id, Object... keyValues) {
        return Status.OK;
    }

    @Override
    public Status writeEdge(String edgeLabel,
                            String outVertexLabel, Object outVertexId,
                            String inVertexLabel, Object inVertexId,
                            Object... keyValues) {
        return Status.OK;
    }

    @Override
    public Status subgraph(QueriesSpec.Query query, Subgraph subgraph) {
        return Status.OK;
    }

}
