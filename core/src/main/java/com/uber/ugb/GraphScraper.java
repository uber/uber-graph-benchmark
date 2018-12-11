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

package com.uber.ugb;

import com.uber.ugb.db.DB;
import com.uber.ugb.db.QueryResult;
import com.uber.ugb.db.Subgraph;
import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.util.ProgressReporter;

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

        String queryText = db.supportedQueryType().equals(query.queryType)
            ? query.queryText : null;

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
                            QueryResult result = null;
                            if (queryText != null) {
                                result = db.executeQuery(queryText, subgraph.startVertexId);
                            } else {
                                db.subgraph(query, subgraph);
                                result = new QueryResult(subgraph.getVertexCount(), subgraph.getEdgeCount());
                            }
                            db.getMetrics().subgraphVertexCount.addAndGet(result.getVertexCount());
                            db.getMetrics().subgraphEdgeCount.addAndGet(result.getEdgeCount());
                            db.getMetrics().subgraphWithEdgesCount.addAndGet(result.getEdgeCount() > 0 ? 1 : 0);
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
