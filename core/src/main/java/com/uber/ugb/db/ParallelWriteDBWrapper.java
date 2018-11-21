package com.uber.ugb.db;

import com.uber.ugb.measurement.Metrics;
import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.schema.Vocabulary;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/*
 * ParallelWriteDBWrapper wraps a DB instance and parallelizes the writes
 */
public class ParallelWriteDBWrapper extends DB {

    private DB db;
    private int concurrency;
    private ArrayBlockingQueue todos;
    private AtomicLong todoCounter;
    private AtomicLong runnableCounter;
    private AtomicBoolean isClosing;
    private ExecutorService executorService;

    public ParallelWriteDBWrapper(DB db, int concurrency) {
        this.db = db;
        this.concurrency = concurrency;
        this.todos = new ArrayBlockingQueue(concurrency);
        this.todoCounter = new AtomicLong();
        this.runnableCounter = new AtomicLong();
        this.isClosing = new AtomicBoolean();
        this.executorService = Executors.newFixedThreadPool(concurrency);
    }

    @Override
    public Vocabulary getVocabulary() {
        return this.db.getVocabulary();
    }

    @Override
    public void setVocabulary(Vocabulary vocabulary) {
        this.db.setVocabulary(vocabulary);
    }

    @Override
    public Metrics getMetrics() {
        return this.db.getMetrics();
    }

    @Override
    public void setMetrics(Metrics metrics) {
        this.db.setMetrics(metrics);
    }

    @Override
    public void init() throws DBException {
        this.db.setMetrics(new Metrics());
        this.db.init();
    }

    @Override
    public void cleanup() throws DBException {
        this.db.cleanup();
    }

    @Override
    public Status commitBatch() {
        return this.db.commitBatch();
    }

    @Override
    public Properties getProperties() {
        return this.db.getProperties();
    }

    @Override
    public void setProperties(Properties p) {
        this.db.setProperties(p);
    }

    @Override
    public Object genVertexId(QualifiedName label, long id) {
        return this.db.genVertexId(label, id);
    }

    public void startup() {
        for (int i = 0; i < concurrency; i++) {
            runnableCounter.incrementAndGet();
            executorService.execute(() -> {
                try {
                    while (true) {
                        Object todo = null;
                        try {
                            todo = todos.poll(500, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        if (todo == null) {
                            if (this.isClosing.get() && this.todoCounter.get() <= 0) {
                                break;
                            } else {
                                continue;
                            }
                        }
                        if (todo instanceof VertexWriteRequest) {
                            VertexWriteRequest request = (VertexWriteRequest) todo;
                            this.db.getMetrics().writeVertex.measure(() -> {
                                this.db.writeVertex(request.label, request.id, request.keyValues);
                            });
                            this.todoCounter.decrementAndGet();
                        }
                        if (todo instanceof EdgeWriteRequest) {
                            EdgeWriteRequest request = (EdgeWriteRequest) todo;
                            this.db.getMetrics().writeEdge.measure(() -> {
                                this.db.writeEdge(request.edgeLabel,
                                    request.outVertexLabel, request.outVertexId,
                                    request.inVertexLabel, request.inVertexId,
                                    request.keyValues);
                            });
                            this.todoCounter.decrementAndGet();
                        }
                    }
                } finally {
                    runnableCounter.decrementAndGet();
                }
            });
        }

    }

    public void shutdown() {
        this.isClosing.set(true);

        while (runnableCounter.get() > 0) {
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

    @Override
    public Status writeVertex(QualifiedName label, Object id, Object... keyValues) {
        try {
            this.todoCounter.incrementAndGet();
            this.todos.put(new VertexWriteRequest(label, id, keyValues));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Status.OK;
    }

    @Override
    public Status writeEdge(QualifiedName edgeLabel,
                            QualifiedName outVertexLabel, Object outVertexId,
                            QualifiedName inVertexLabel, Object inVertexId,
                            Object... keyValues) {
        try {
            this.todoCounter.incrementAndGet();
            this.todos.put(new EdgeWriteRequest(
                edgeLabel,
                outVertexLabel, outVertexId,
                inVertexLabel, inVertexId,
                keyValues));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Status.OK;
    }

    @Override
    public Status subgraph(QueriesSpec.Query query, Subgraph subgraph) {
        return Status.NOT_IMPLEMENTED;
    }

    class VertexWriteRequest {
        QualifiedName label;
        Object id;
        Object[] keyValues;

        public VertexWriteRequest(QualifiedName label, Object id, Object[] keyValues) {
            this.label = label;
            this.id = id;
            this.keyValues = keyValues;
        }
    }

    class EdgeWriteRequest {
        QualifiedName edgeLabel;
        QualifiedName outVertexLabel;
        Object outVertexId;
        QualifiedName inVertexLabel;
        Object inVertexId;
        Object[] keyValues;

        public EdgeWriteRequest(QualifiedName edgeLabel,
                                QualifiedName outVertexLabel, Object outVertexId,
                                QualifiedName inVertexLabel, Object inVertexId,
                                Object[] keyValues) {
            this.edgeLabel = edgeLabel;
            this.outVertexLabel = outVertexLabel;
            this.outVertexId = outVertexId;
            this.inVertexLabel = inVertexLabel;
            this.inVertexId = inVertexId;
            this.keyValues = keyValues;
        }
    }

}
