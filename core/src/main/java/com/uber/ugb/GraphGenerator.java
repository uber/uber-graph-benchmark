package com.uber.ugb;

import com.google.common.base.Preconditions;
import com.uber.ugb.db.DB;
import com.uber.ugb.db.DBException;
import com.uber.ugb.db.ParallelWriteDBWrapper;
import com.uber.ugb.measurement.Metrics;
import com.uber.ugb.model.EdgeModel;
import com.uber.ugb.model.GraphModel;
import com.uber.ugb.model.PropertyModel;
import com.uber.ugb.model.SimpleProperty;
import com.uber.ugb.model.distro.DegreeDistribution;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.util.ProgressReporter;
import com.uber.ugb.util.RandomPermutation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
        long domainExistCount = (long) (edgeStats.getDomainIncidence().getExistenceProbability() * domainSize);
        long rangeExistCount = (long) (edgeStats.getRangeIncidence().getExistenceProbability() * rangeSize);

        int domainBucketWidth = domainSize > 1024 * 1024 ? 1024 : 1;
        int rangeBucketWidth = rangeSize > 1024 * 1024 ? 1024 : 1;
        int domainBucketCount = (int) (domainSize / domainBucketWidth);
        int rangeBucketCount = (int) (rangeSize / rangeBucketWidth);

        // subset of domain vertices and range vertices which will have at least one edge of the given label
        RandomSubset<Integer> domainSubset = createRandomSubset(
            new DirectSet(domainBucketCount), edgeStats.getDomainIncidence().getExistenceProbability(), random);
        RandomSubset<Integer> rangeSubset = createRandomSubset(
            new DirectSet(rangeBucketCount), edgeStats.getRangeIncidence().getExistenceProbability(), random);

        // domain and range distributions
        DegreeDistribution.Sample domainSample
            = edgeStats.getDomainIncidence().getDegreeDistribution().createSample(domainSubset.size(), random);
        DegreeDistribution.Sample rangeSample
            = edgeStats.getRangeIncidence().getDegreeDistribution().createSample(rangeSubset.size(), random);

        WeightedBuckets domainWeightedBuckets = new WeightedBuckets(domainSample, domainSubset.size());
        WeightedBuckets rangeWeightedBuckets = new WeightedBuckets(rangeSample, rangeSubset.size());

        String prefix = String.format("gen %s(%d/%d):%s:%s(%d/%d)",
            domainLabel, domainExistCount, domainSize,
            edgeLabel,
            rangeLabel, rangeExistCount, rangeSize
        );

        ProgressReporter progressReporter = new ProgressReporter(prefix, start, stop, 102400L);

        for (long edgeCount = start; edgeCount < stop; edgeCount++) {
            // pick tail
            int domainBucket = domainWeightedBuckets.locate(random);
            int tbd = random.nextInt(domainBucketWidth);
            long tailIndex = (long) domainSubset.get(domainBucket) * domainBucketWidth + tbd;

            // pick head
            int rangeBucket = rangeWeightedBuckets.locate(random);
            int hbd = random.nextInt(domainBucketWidth);
            long headIndex = rangeSubset.get(rangeBucket) * rangeBucketWidth + hbd;

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

    private <T> RandomSubset<T> createRandomSubset(final IndexSet<T> base,
                                                   final double probability, final Random random) {
        int size = (int) (probability * base.size());
        return new RandomSubset<>(base, size, random);
    }

    public interface IndexSet<T> {
        int size();

        T get(int index);
    }

    private interface RunnableWithException<E extends Exception> {
        void run() throws E;
    }

    public static class RandomSubset<T> implements IndexSet<T> {
        private final IndexSet<T> base;
        private final int[] permutation;
        private final int size;

        RandomSubset(final IndexSet<T> base, final int size, final Random random) {
            this.base = base;
            this.permutation = new RandomPermutation(base.size(), random).getPermutation();
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public T get(final int index) {
            Preconditions.checkArgument(index < size);
            return base.get(permutation[index]);
        }
    }

    public static class IntervalSet implements IndexSet<Integer> {
        private final int firstIndex;
        private final int size;

        public IntervalSet(int firstIndex, int size) {
            this.firstIndex = firstIndex;
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Integer get(int index) {
            Preconditions.checkArgument(index < size);
            return firstIndex + index;
        }
    }

    public static class DirectSet implements IndexSet<Integer> {
        private final int size;

        public DirectSet(int size) {
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Integer get(int index) {
            Preconditions.checkArgument(index < size);
            return index;
        }
    }

    private static class WeightedBuckets {
        double[] accumulatedWeights;
        double totalWeight;

        public WeightedBuckets(DegreeDistribution.Sample sample, int bucketCount) {
            double[] weights = new double[bucketCount];
            accumulatedWeights = new double[bucketCount];
            totalWeight = 0;
            for (int i = 0; i < bucketCount; i++) {
                weights[i] = sample.getNextDegree();
                totalWeight += weights[i];
            }
            double currentWeight = 0;
            for (int i = 0; i < bucketCount; i++) {
                currentWeight += weights[i];
                accumulatedWeights[i] = currentWeight / totalWeight;
            }
        }

        public int locate(Random random) {
            double r = random.nextDouble();
            int x = Arrays.binarySearch(this.accumulatedWeights, r);
            if (x < 0) {
                x = (-x) - 1;
            }
            return x;
        }

        public double getTotalWeight() {
            return totalWeight;
        }
    }
}
