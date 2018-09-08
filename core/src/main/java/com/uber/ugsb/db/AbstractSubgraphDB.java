package com.uber.ugsb.db;

import com.google.common.base.Strings;
import com.uber.ugsb.queries.QueriesSpec;
import com.uber.ugsb.schema.SchemaManager;
import com.uber.ugsb.schema.model.RelationType;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractSubgraphDB extends DB {

    protected static final ThreadLocal<ThreadPoolExecutor> TL_EXECUTOR_BUILDER = new ThreadLocal<ThreadPoolExecutor>() {
        @Override
        protected ThreadPoolExecutor initialValue() {
            return (ThreadPoolExecutor) Executors.newFixedThreadPool(16);
        }
    };

    protected static ThreadPoolExecutor getThreadPoolExecutor() {
        return TL_EXECUTOR_BUILDER.get();
    }

    public abstract Properties readVertex(String label, Object id, QueriesSpec.Query.Step.Vertex vertexQuerySpec);

    public abstract List<Subgraph.Edge> readEdges(Object startVertexId, QueriesSpec.Query.Step.Edge edgeQuerySpec);

    @Override
    public Status subgraph(QueriesSpec.Query query, Subgraph subgraph) {

        Set<Object> visitedVertexIds = ConcurrentHashMap.newKeySet();
        LinkedBlockingQueue<TODO> todos = new LinkedBlockingQueue<>();
        int stepCount = query.steps.length;
        if (stepCount == 0) {
            return Status.OK;
        }

        ThreadPoolExecutor executorService = getThreadPoolExecutor();
        AtomicInteger waitGroup = new AtomicInteger();

        try {
            todos.put(new TODO(subgraph.startVertexId, 0, null));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        AtomicBoolean hasException = new AtomicBoolean();

        while ((!todos.isEmpty() || waitGroup.get() > 0) && !hasException.get()) {
            if (todos.isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
                continue;
            }

            TODO todo = todos.poll();
            if (todo.currentEdge != null) {
                waitGroup.incrementAndGet();
                executorService.execute(() -> {
                    try {
                        processVertexToDo(todo, query.steps[todo.stepId], todo.currentEdge);
                    } catch (Exception e) {
                        e.printStackTrace();
                        hasException.set(true);
                    } finally {
                        waitGroup.decrementAndGet();
                    }
                });
            } else {
                waitGroup.incrementAndGet();
                executorService.execute(() -> {
                    try {
                        boolean isLastStep = todo.stepId + 1 >= query.steps.length;
                        QueriesSpec.Query.Step step = query.steps[todo.stepId];
                        traverseOneStep(todo.id, step, visitedVertexIds, todos, subgraph, todo.stepId, isLastStep);
                    } catch (Exception e) {
                        e.printStackTrace();
                        hasException.set(true);
                    } finally {
                        waitGroup.decrementAndGet();
                    }
                });
            }
        }

        return Status.OK;
    }

    private void processVertexToDo(TODO todo, QueriesSpec.Query.Step step, Subgraph.Edge subgraphEdge) {

        RelationType relationType = vocabulary.getRelationType(step.edge.label, SchemaManager.TypeCategory.Relation);
        String vertexLabel = step.edge.isBackward()
            ? relationType.getFrom().getLabel()
            : relationType.getTo().getLabel();

        this.getMetrics().readVertex.measure(() -> {
            Properties vertexProperties = readVertex(vertexLabel, todo.id, step.vertex);
            vertexProperties = extractProperties(vertexProperties, step.vertex.select, null);
            subgraphEdge.setVertexProperties(vertexProperties);
        });
    }

    public void traverseOneStep(Object id, QueriesSpec.Query.Step step, Set<Object> visitedVertexIds,
                                LinkedBlockingQueue<TODO> todos, Subgraph subgraph,
                                int stepId, boolean isLastStep) throws Exception {

        this.getMetrics().readEdge.measure(() -> {
            try {
                int edgeCounter = 0;

                List<Subgraph.Edge> possibleEdges = readEdges(id, step.edge);

                for (Subgraph.Edge edge : possibleEdges) {
                    if (!step.edge.matchEdgeFilter(edge.edgeProperties)) {
                        // this edge is filtered out
                        continue;
                    }
                    edgeCounter++;
                    if (step.edge.limit > 0 && edgeCounter >= step.edge.limit) {
                        // no more edge is needed
                        return;
                    }
                    if (!visitedVertexIds.add(edge.nextVertexId)) {
                        // this new vertex id has already been visited
                        continue;
                    }

                    subgraph.addEdge(edge);

                    if (step.vertex != null && !Strings.isNullOrEmpty(step.vertex.select)) {
                        todos.put(new AbstractSubgraphDB.TODO(edge.nextVertexId, stepId, edge));
                    }
                    if (!isLastStep) {
                        todos.put(new AbstractSubgraphDB.TODO(edge.nextVertexId, stepId + 1, null));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });

    }

    private static class TODO {
        Object id;
        int stepId;
        Subgraph.Edge currentEdge;

        TODO(Object id, int stepId, Subgraph.Edge currentEdge) {
            this.id = id;
            this.stepId = stepId;
            this.currentEdge = currentEdge;
        }
    }

}
