package com.uber.ugb;

import com.google.common.base.Preconditions;
import com.uber.ugb.db.DB;
import com.uber.ugb.db.DBException;
import com.uber.ugb.db.ParallelWriteDBWrapper;
import com.uber.ugb.measurement.Metrics;
import com.uber.ugb.model.BucketedEdgeDistribution;
import com.uber.ugb.model.EdgeModel;
import com.uber.ugb.model.GraphModel;
import com.uber.ugb.model.PropertyModel;
import com.uber.ugb.model.SimpleProperty;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.util.ProgressReporter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * A utility which creates property graphs in accordance with a given statistical model.
 * By design, the model is based on a "real" production graph; the generated graphs serve as unchanging and
 * reproducible substitutes for the real graph in tests and benchmarks.
 * Also by design, the statistics of the generated graph are independent of the scale of the graph,
 * which is measured in number of vertices.
 */
public class GraphGenerator implements Serializable {

    private static Logger logger = Logger.getLogger(GraphGenerator.class.getName());
    private final GraphModel model;
    // used in random permutations and degree distributions
    protected long randomSeed;
    private Map<QualifiedName, Long> vertexPartition;
    private long batchSize = 1000;
    private int batchCounter = 0;

    /**
     * Creates a new generator with the given statistical model
     *
     * @param model the statistical model which generated graphs are to follow
     */
    public GraphGenerator(final GraphModel model) {
        this.model = model;
        setRandomSeed(new Random().nextLong());
    }

    public GraphModel getModel() {
        return model;
    }

    public Map<QualifiedName, Long> getVertexPartition() {
        return vertexPartition;
    }

    /**
     * Sets the batch commit size of this graph generator. Grouping multiple graph mutations in a single commit
     * typically results in better performance. As the batch size increases, however, the added value of
     * an even larger batch size eventually drops to zero.
     *
     * @param batchSize the number of basic operations to be included in each commit. Basic operations include
     *                  the creation of a vertex, edge, or property.
     */
    synchronized void setBatchSize(final long batchSize) {
        this.batchSize = batchSize;
        batchCounter = 0;
    }

    /**
     * Seeds the random number generator. If no seed is provided, a new seed is chosen for each invocation.
     * When a seed is provided, it completely determines the graph; subsequent invocations using the same seed,
     * model, and size will always produce the same graph.
     *
     * @param randomSeed the seed value
     */
    public void setRandomSeed(final long randomSeed) {
        this.randomSeed = randomSeed;
    }

    /**
     * Generates a graph with a given number of vertices.
     *
     * @param graph         the property graph to which generated elements are to be added
     * @param totalVertices the size of the generated graph, in terms of vertices
     */
    public synchronized Metrics generateTo(final DB graph, final long totalVertices, final int writeConcurrency,
                                           final int graphPartitionCount) throws DBException {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(totalVertices > 0);
        Preconditions.checkArgument(writeConcurrency > 0);
        Preconditions.checkArgument(graphPartitionCount > 0);

        ParallelWriteDBWrapper pdb = new ParallelWriteDBWrapper(graph, writeConcurrency);
        pdb.init();
        pdb.startup();


        vertexPartition = model.getVertexPartitioner().getPartitionSizes(totalVertices);

        long vertexPartitionSize = Math.floorDiv(totalVertices, graphPartitionCount) + 1;
        long totalEdges = countTotalEdges();
        long edgePartitionSize = Math.floorDiv(totalEdges, graphPartitionCount) + 1;

        long time = timeTask(() -> {
            for (long i = 0; i < graphPartitionCount; i++) {
                long start = i * vertexPartitionSize;
                long stop = Math.min(i * vertexPartitionSize + vertexPartitionSize, totalVertices);
                createVertices(start, stop, pdb);
            }
            for (long i = 0; i < graphPartitionCount; i++) {
                long start = i * edgePartitionSize;
                long stop = Math.min(i * edgePartitionSize + edgePartitionSize, totalEdges);
                createEdges(start, stop, pdb, new Random(randomSeed + i));
            }
            commit(pdb);
        });
        logger.info("generated graph of " + totalVertices + " vertices in " + time + "ms");

        pdb.shutdown();

        pdb.cleanup();

        return pdb.getMetrics();
    }

    /**
     * Generates a graph with a given number of vertices.
     *
     * @param graph         the property graph to which generated elements are to be added
     * @param totalVertices the size of the generated graph, in terms of vertices
     */
    public synchronized Metrics generateTo(SparkConf sparkConf,
                                           final DB graph, final long totalVertices,
                                           final int writeConcurrency, final int graphPartitionCount) {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(totalVertices > 0);
        Preconditions.checkArgument(writeConcurrency > 0);
        Preconditions.checkArgument(graphPartitionCount > 0);

        vertexPartition = model.getVertexPartitioner().getPartitionSizes(totalVertices);

        long vertexPartitionSize = Math.floorDiv(totalVertices, graphPartitionCount) + 1;
        long totalEdges = countTotalEdges();
        long edgePartitionSize = Math.floorDiv(totalEdges, graphPartitionCount) + 1;

        List<Integer> parts = new ArrayList<>();
        for (int i = 0; i < graphPartitionCount; i++) {
            parts.add(i);
        }

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        System.out.println("start generating " + totalVertices + " vertices by partition size " + vertexPartitionSize);
        System.out.println("start generating " + totalEdges + " edges by partition size " + edgePartitionSize);

        JavaRDD<Metrics> genVertex = javaSparkContext.parallelize(parts)
            .repartition(graphPartitionCount)
            .map((Function<Integer, Metrics>) i -> {

                ParallelWriteDBWrapper pdb = new ParallelWriteDBWrapper(graph, writeConcurrency);
                pdb.init();
                pdb.startup();

                long start = i * vertexPartitionSize;
                long stop = Math.min(i * vertexPartitionSize + vertexPartitionSize, totalVertices);
                createVertices(start, stop, pdb);
                commit(pdb);

                pdb.shutdown();
                Metrics currentMetrics = pdb.getMetrics();
                pdb.cleanup();
                return currentMetrics;
            });

        Metrics finalMetrics = genVertex
            .zipWithIndex()
            .repartition(graphPartitionCount)
            .map((Function<Tuple2<Metrics, Long>, Metrics>) t -> {

                ParallelWriteDBWrapper pdb = new ParallelWriteDBWrapper(graph, writeConcurrency);
                pdb.init();
                pdb.startup();

                Metrics prevMetrics = t._1;
                long i = t._2;
                long start = i * edgePartitionSize;
                long stop = Math.min(i * edgePartitionSize + edgePartitionSize, totalEdges);
                createEdges(start, stop, pdb, new Random(randomSeed + i));
                commit(pdb);
                pdb.shutdown();
                Metrics currentMetrics = pdb.getMetrics();
                pdb.cleanup();

                return prevMetrics.merge(currentMetrics);
            }).reduce((m1, m2) -> m1.merge(m2));

        javaSparkContext.close();

        return finalMetrics;

    }

    private <E extends Exception> long timeTask(final RunnableWithException<E> task) throws E {
        long startTime = System.currentTimeMillis();
        task.run();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    private void incrementBatchCounter(final DB graph) {
        batchCounter++;
        if (batchSize == batchCounter) {
            commit(graph);
            batchCounter = 0;
        }
    }

    private void commit(final DB graph) {
        graph.getMetrics().batchCommit.measure(() -> {
            graph.commitBatch();
        });
    }

    private void createVertices(long start, long stop, final DB graph) {
        long counter = 0;
        for (Map.Entry<QualifiedName, Long> e : vertexPartition.entrySet()) {
            QualifiedName label = e.getKey();
            PropertyModel props = model.getVertexPropertyModels().get(label);
            long nVertices = e.getValue();
            long partitionedStart = Math.max(counter, start);
            long partitionedStop = Math.min(counter + nVertices, stop);
            counter += nVertices;
            if (partitionedStart >= partitionedStop) {
                continue;
            }
            logger.info("generating [" + partitionedStart + ", " + partitionedStop + ") " + nVertices + " " + label);
            ProgressReporter progressReporter = new ProgressReporter("gen vertex " + label, start, stop, 102400L);
            for (long i = partitionedStart; i < partitionedStop; i++) {
                createVertex(label, i, graph, props);
                progressReporter.maybeReport(i);
            }
            progressReporter.report(stop);
        }
    }

    private void createEdges(long start, long stop, final DB graph, final Random random) {
        long counter = 0;
        for (Map.Entry<QualifiedName, EdgeModel> e : model.getEdgeModels().entrySet()) {
            QualifiedName edgeLabel = e.getKey();
            EdgeModel edgeStats = e.getValue();
            long nEdges = getEdgeCount(edgeStats);
            long partitionedStart = Math.max(counter, start);
            long partitionedStop = Math.min(counter + nEdges, stop);
            counter += nEdges;
            if (partitionedStart >= partitionedStop) {
                continue;
            }
            logger.info("generating [" + partitionedStart + ", " + partitionedStop + ") " + nEdges + " " + edgeLabel);
            createEdgesForEdgeModel(edgeLabel, edgeStats, graph, partitionedStart, partitionedStop, random);
        }
    }

    private long countTotalEdges() {
        long counter = 0;
        for (Map.Entry<QualifiedName, EdgeModel> e : model.getEdgeModels().entrySet()) {
            EdgeModel edgeStats = e.getValue();
            long nEdges = getEdgeCount(edgeStats);
            counter += nEdges;
        }
        return counter;
    }

    private long getEdgeCount(EdgeModel edgeStats) {
        QualifiedName domainLabel = edgeStats.getDomainIncidence().getVertexLabel();
        QualifiedName rangeLabel = edgeStats.getRangeIncidence().getVertexLabel();
        long domainSize = vertexPartition.get(domainLabel);
        long rangeSize = vertexPartition.get(rangeLabel);
        long outEdges = (long) (edgeStats.getDomainIncidence().getExistenceProbability() * domainSize);
        long inEdges = (long) (edgeStats.getRangeIncidence().getExistenceProbability() * rangeSize);
        return Math.max(outEdges, inEdges);
    }

    private void createEdgesForEdgeModel(final QualifiedName edgeLabel, final EdgeModel edgeStats,
                                         final DB graph, final long start, final long stop, final Random random) {

        QualifiedName domainLabel = edgeStats.getDomainIncidence().getVertexLabel();
        QualifiedName rangeLabel = edgeStats.getRangeIncidence().getVertexLabel();

        long domainSize = vertexPartition.get(domainLabel);
        long rangeSize = vertexPartition.get(rangeLabel);

        BucketedEdgeDistribution domainBucketDistribution = new BucketedEdgeDistribution(
            edgeStats.getDomainIncidence(), domainSize, random);
        BucketedEdgeDistribution rangeBucketDistribution = new BucketedEdgeDistribution(
            edgeStats.getRangeIncidence(), rangeSize, random);

        // prefix format of the print out
        long domainExistCount = (long) (edgeStats.getDomainIncidence().getExistenceProbability() * domainSize);
        long rangeExistCount = (long) (edgeStats.getRangeIncidence().getExistenceProbability() * rangeSize);
        String prefix = String.format("gen %s(%d/%d):%s:%s(%d/%d)",
            domainLabel, domainExistCount, domainSize,
            edgeLabel,
            rangeLabel, rangeExistCount, rangeSize
        );

        ProgressReporter progressReporter = new ProgressReporter(prefix, start, stop, 102400L);

        for (long edgeCount = start; edgeCount < stop; edgeCount++) {
            long tailIndex = domainBucketDistribution.pickOne();
            long headIndex = rangeBucketDistribution.pickOne();
            createEdge(edgeLabel, domainLabel, tailIndex, rangeLabel, headIndex, graph);
            progressReporter.maybeReport(edgeCount);
        }

        progressReporter.report(stop);
    }

    private void createVertex(final QualifiedName label, long id, final DB graph, final PropertyModel props) {

        Object vertexId = graph.genVertexId(label, id);
        Object[] params = new Object[(props == null ? 0 : props.getProperties().size() * 2)];

        if (null != props) {
            int i = 0;
            for (SimpleProperty prop : props.getProperties()) {
                params[i] = prop.getKey();
                Object value = prop.getValueGenerator().generate(this.randomSeed, label.toString(), id, prop.getKey());
                params[i + 1] = value;
                i += 2;
            }
        }
        graph.writeVertex(label, vertexId, params);
        incrementBatchCounter(graph);
    }

    private void createEdge(final QualifiedName label,
                            QualifiedName tailLabel, final long tailIndex,
                            QualifiedName headLabel, final long headIndex,
                            final DB graph) {
        Object tailId = graph.genVertexId(tailLabel, tailIndex);
        Object headId = graph.genVertexId(headLabel, headIndex);

        //System.out.println(String.format("gen %s(%d:%d) %s %s(%d:%d)",
        //    tailLabel, tailIndex, tailId,
        //    label,
        //    headLabel, headIndex, headId
        //));
        graph.writeEdge(label, tailLabel, tailId, headLabel, headId);

        incrementBatchCounter(graph);
    }

    private interface RunnableWithException<E extends Exception> {
        void run() throws E;
    }

}
