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

package com.uber.ugb.measurement;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics implements Serializable {
    public LatencyHistogram writeVertex;
    public LatencyHistogram writeEdge;
    public LatencyHistogram batchCommit;
    public LatencyHistogram readVertex;
    public LatencyHistogram readEdge;
    public LatencyHistogram subgraph;
    public AtomicLong subgraphVertexCount;
    public AtomicLong subgraphEdgeCount;
    public AtomicLong subgraphWithEdgesCount;

    public Metrics() {
        this.writeVertex = new LatencyHistogram("write.vertex");
        this.writeEdge = new LatencyHistogram("write.edge");
        this.batchCommit = new LatencyHistogram("batch.commit");
        this.readVertex = new LatencyHistogram("read.vertex");
        this.readEdge = new LatencyHistogram("read.edge");
        this.subgraph = new LatencyHistogram("subgraph");
        this.subgraphVertexCount = new AtomicLong();
        this.subgraphEdgeCount = new AtomicLong();
        this.subgraphWithEdgesCount = new AtomicLong();
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
        json.add("non.empty.subgraph.count", new JsonPrimitive(subgraphWithEdgesCount.get()));

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        Writer writer = new BufferedWriter(new OutputStreamWriter(out));
        gson.toJson(json, writer);
        writer.close();
    }

    public void collectMetrics(JsonMetricsOutput jsonOutput, LatencyHistogram m) throws IOException {
        if (m.hasData()) {
            m.printout(jsonOutput);
        }
    }

    public Metrics merge(Metrics that) {

        this.writeVertex.merge(that.writeVertex);
        this.writeEdge.merge(that.writeEdge);
        this.batchCommit.merge(that.batchCommit);
        this.readVertex.merge(that.readVertex);
        this.readEdge.merge(that.readEdge);
        this.subgraph.merge(that.subgraph);
        this.subgraphVertexCount.addAndGet(that.subgraphVertexCount.get());
        this.subgraphEdgeCount.addAndGet(that.subgraphEdgeCount.get());
        this.subgraphWithEdgesCount.addAndGet(that.subgraphWithEdgesCount.get());

        return this;
    }
}
