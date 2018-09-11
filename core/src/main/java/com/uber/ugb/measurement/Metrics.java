package com.uber.ugb.measurement;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics {
    public Measurement writeVertex;
    public Measurement writeEdge;
    public Measurement batchCommit;
    public Measurement readVertex;
    public Measurement readEdge;
    public Measurement subgraph;
    public AtomicLong subgraphVertexCount;
    public AtomicLong subgraphEdgeCount;

    public Metrics() {
        this.writeVertex = new LatencyHistogram("write.vertex");
        this.writeEdge = new LatencyHistogram("write.edge");
        this.batchCommit = new LatencyHistogram("batch.commit");
        this.readVertex = new LatencyHistogram("read.vertex");
        this.readEdge = new LatencyHistogram("read.edge");
        this.subgraph = new LatencyHistogram("subgraph");
        this.subgraphVertexCount = new AtomicLong();
        this.subgraphEdgeCount = new AtomicLong();
    }

    public void printOut(OutputStream out) throws IOException {

        JsonMetricsOutput jsonOutput = new JsonMetricsOutput();
        collectMetrics(jsonOutput, this.writeVertex);
        collectMetrics(jsonOutput, this.writeEdge);
        collectMetrics(jsonOutput, this.batchCommit);
        collectMetrics(jsonOutput, this.readVertex);
        collectMetrics(jsonOutput, this.readEdge);
        collectMetrics(jsonOutput, this.subgraph);

        JsonObject json = jsonOutput.getJson();
        json.add("subgraph.vertex.count", new JsonPrimitive(subgraphVertexCount.get()));
        json.add("subgraph.edge.count", new JsonPrimitive(subgraphEdgeCount.get()));

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        Writer writer = new BufferedWriter(new OutputStreamWriter(out));
        gson.toJson(json, writer);
        writer.close();
    }

    public void collectMetrics(JsonMetricsOutput jsonOutput, Measurement m) throws IOException {
        if (m.hasData()) {
            m.printout(jsonOutput);
        }
    }

}
