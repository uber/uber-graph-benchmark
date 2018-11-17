package com.uber.ugb;

import com.uber.ugb.db.DB;
import com.uber.ugb.db.QueryCapability;
import com.uber.ugb.db.Subgraph;
import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.util.ProgressReporter;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import javax.script.ScriptException;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class GraphScraper {

    private static Logger logger = Logger.getLogger(GraphScraper.class.getName());

    public GraphScraper() {
    }

    public void scrape(DB db, int seed, long outVertexCount,
                       QueriesSpec.Query query,
                       long operationCount, int concurrency) {

        ArrayBlockingQueue<Task> tasks = new ArrayBlockingQueue(concurrency * 16);
        AtomicLong readCounter = new AtomicLong();
        AtomicBoolean hasException = new AtomicBoolean();

        ExecutorService executorService = Executors.newFixedThreadPool(concurrency + 1);
        executorService.execute(() -> {
            Random random = new Random(seed);
            AtomicLong y = new AtomicLong();
            QualifiedName startVertexName = new QualifiedName(query.startVertexLabel);
            random.longs(operationCount, 0, outVertexCount).forEach(x -> {
                try {
                    tasks.put(new Task(db.genVertexId(startVertexName, x), y.incrementAndGet()));
                } catch (InterruptedException e) {
                }
            });
        });

        ProgressReporter progressReporter = new ProgressReporter("query", 0, operationCount, 100L);

        AtomicLong queryCount = new AtomicLong();

        for (int i = 0; i < concurrency; i++) {
            executorService.execute(() -> {
                while (readCounter.get() < operationCount && !hasException.get()) {
                    Task task = null;
                    try {
                        task = tasks.poll(100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                    }
                    if (task == null) {
                        continue;
                    }
                    Subgraph subgraph = new Subgraph(task.vid);
                    try {
                        db.getMetrics().subgraph.measure(() -> {
                            int vertexCount = 0, edgeCount = 0;
                            if (db instanceof QueryCapability.SupportGremlin) {
                                try {
                                    TinkerGraph g2 = (TinkerGraph) ((QueryCapability.SupportGremlin) db).queryByGremlin(
                                        query.gremlinQuery, "x", subgraph.startVertexId);
                                    vertexCount = g2.traversal().V().count().next().intValue();
                                    edgeCount = g2.traversal().E().count().next().intValue();
                                } catch (ScriptException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException(e);
                                }
                            } else {
                                db.subgraph(query, subgraph);
                                vertexCount = subgraph.getVertexCount();
                                edgeCount = subgraph.getEdgeCount();
                            }
                            db.getMetrics().subgraphVertexCount.addAndGet(vertexCount);
                            db.getMetrics().subgraphEdgeCount.addAndGet(edgeCount);
                            db.getMetrics().subgraphWithEdgesCount.addAndGet(edgeCount > 0 ? 1 : 0);
                        });
                        readCounter.addAndGet(1);
                    } catch (Exception e) {
                        e.printStackTrace();
                        hasException.set(true);
                        return;
                    }

                    queryCount.incrementAndGet();

                    progressReporter.maybeReport(queryCount.get());

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

        progressReporter.report(operationCount);

        executorService.shutdownNow();
        try {
            while (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

    }

    class Task {
        Object vid;
        long seqId;

        public Task(Object vid, long seqId) {
            this.vid = vid;
            this.seqId = seqId;
        }
    }

}
