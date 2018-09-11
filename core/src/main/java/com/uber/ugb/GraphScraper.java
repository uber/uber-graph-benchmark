package com.uber.ugb;

import com.uber.ugb.db.DB;
import com.uber.ugb.db.QueryCapability;
import com.uber.ugb.db.Subgraph;
import com.uber.ugb.queries.QueriesSpec;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import javax.script.ScriptException;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class GraphScraper {

    public GraphScraper() {
    }

    public void scrape(DB db, int seed, int outVertexCount,
                       QueriesSpec.Query query,
                       int operationCount, int concurrency) {

        ArrayBlockingQueue todos = new ArrayBlockingQueue(concurrency * 16);
        AtomicLong readCounter = new AtomicLong();
        AtomicBoolean hasException = new AtomicBoolean();

        ExecutorService executorService = Executors.newFixedThreadPool(concurrency + 1);
        executorService.execute(() -> {
            Random random = new Random(seed);
            for (int i = 0; i < operationCount; i++) {
                int index = random.nextInt(outVertexCount);
                try {
                    todos.put(db.genVertexId(query.startVertexLabel, index));
                } catch (InterruptedException e) {
                    continue;
                }
            }
        });

        for (int i = 0; i < concurrency; i++) {
            executorService.execute(() -> {
                while (readCounter.get() < operationCount) {
                    Object startVertexId = null;
                    try {
                        startVertexId = todos.poll(100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (startVertexId == null) {
                        continue;
                    }
                    Subgraph subgraph = new Subgraph(startVertexId);
                    try {
                        db.getMetrics().subgraph.measure(() -> {
                            if (db instanceof QueryCapability.SupportGremlin) {
                                try {
                                    TinkerGraph g2 = (TinkerGraph) ((QueryCapability.SupportGremlin) db).queryByGremlin(
                                        query.gremlinQuery, "x", subgraph.startVertexId);
                                    db.getMetrics().subgraphVertexCount.addAndGet(
                                        g2.traversal().V().count().next().intValue());
                                    db.getMetrics().subgraphEdgeCount.addAndGet(
                                        g2.traversal().E().count().next().intValue());
                                } catch (ScriptException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException(e);
                                }
                            } else {
                                db.subgraph(query, subgraph);
                            }
                        });
                        db.getMetrics().subgraphVertexCount.addAndGet(subgraph.getVertexCount());
                        db.getMetrics().subgraphEdgeCount.addAndGet(subgraph.getEdgeCount());
                        readCounter.addAndGet(1);
                    } catch (Exception e) {
                        e.printStackTrace();
                        hasException.set(true);
                        return;
                    }
                }
            });
        }

        while (readCounter.get() < operationCount && !hasException.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        executorService.shutdownNow();
        try {
            while (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

    }
}
