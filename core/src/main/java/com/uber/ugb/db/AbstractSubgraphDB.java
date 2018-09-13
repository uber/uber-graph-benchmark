package com.uber.ugb.db;

import com.google.common.base.Strings;
import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.SchemaManager;
import com.uber.ugb.schema.model.RelationType;

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
        LinkedBlockingQueue<Task> tasks = new LinkedBlockingQueue<>();
        int stepCount = query.steps.length;
        if (stepCount == 0) {
            return Status.OK;
        }

        ThreadPoolExecutor executorService = getThreadPoolExecutor();
        AtomicInteger waitGroup = new AtomicInteger();

        try {
            tasks.put(new Task(subgraph.startVertexId, 0, null));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        AtomicBoolean hasException = new AtomicBoolean();

        while ((!tasks.isEmpty() || waitGroup.get() > 0) && !hasException.get()) {
            if (tasks.isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
                continue;
            }

            Task task = tasks.poll();
            if (task.currentEdge != null) {
                waitGroup.incrementAndGet();
                executorService.execute(() -> {
                    try {
                        processVertexToDo(task, query.steps[task.stepId], task.currentEdge);
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
                        boolean isLastStep = task.stepId + 1 >= query.steps.length;
                        QueriesSpec.Query.Step step = query.steps[task.stepId];
                        traverseOneStep(task.id, step, visitedVertexIds, tasks, subgraph, task.stepId, isLastStep);
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

    private void processVertexToDo(Task task, QueriesSpec.Query.Step step, Subgraph.Edge subgraphEdge) {

        RelationType relationType = vocabulary.getRelationType(step.edge.label, SchemaManager.TypeCategory.Relation);
        String vertexLabel = step.edge.isBackward()
            ? relationType.getFrom().getLabel()
            : relationType.getTo().getLabel();

        this.getMetrics().readVertex.measure(() -> {
            Properties vertexProperties = readVertex(vertexLabel, task.id, step.vertex);
            vertexProperties = extractProperties(vertexProperties, step.vertex.select, null);
            subgraphEdge.setVertexProperties(vertexProperties);
        });
    }

    public void traverseOneStep(Object id, QueriesSpec.Query.Step step, Set<Object> visitedVertexIds,
                                LinkedBlockingQueue<Task> tasks, Subgraph subgraph,
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
                        tasks.put(new Task(edge.nextVertexId, stepId, edge));
                    }
                    if (!isLastStep) {
                        tasks.put(new Task(edge.nextVertexId, stepId + 1, null));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });

    }

    private static class Task {
        Object id;
        int stepId;
        Subgraph.Edge currentEdge;

        Task(Object id, int stepId, Subgraph.Edge currentEdge) {
            this.id = id;
            this.stepId = stepId;
            this.currentEdge = currentEdge;
        }
    }

}
