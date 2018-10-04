package com.uber.ugb.db;

import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;

/*
 * NoopDB does nothing and only returns Status.OK for every operations.
 */
public class NoopDB extends DB {

    @Override
    public Status writeVertex(QualifiedName label, Object id, Object... keyValues) {
        return Status.OK;
    }

    @Override
    public Status writeEdge(QualifiedName edgeLabel,
                            QualifiedName outVertexLabel, Object outVertexId,
                            QualifiedName inVertexLabel, Object inVertexId,
                            Object... keyValues) {
        return Status.OK;
    }

    @Override
    public Status subgraph(QueriesSpec.Query query, Subgraph subgraph) {
        return Status.OK;
    }

}
