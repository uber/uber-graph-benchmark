package com.uber.ugb.db;

public class QueryResult {
    int vertexCount;
    int edgeCount;

    public QueryResult(int vertexCount, int edgeCount) {
        this.vertexCount = vertexCount;
        this.edgeCount = edgeCount;
    }

    public int getVertexCount() {
        return vertexCount;
    }

    public int getEdgeCount() {
        return edgeCount;
    }
}
