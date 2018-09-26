package com.uber.ugb;

import com.google.common.base.Preconditions;
import com.uber.ugb.db.DB;
import com.uber.ugb.model.EdgeModel;
import com.uber.ugb.model.GraphModel;
import com.uber.ugb.model.PropertyModel;
import com.uber.ugb.model.SimpleProperty;
import com.uber.ugb.model.distro.DegreeDistribution;
import com.uber.ugb.util.ProgressReporter;
import com.uber.ugb.util.RandomPermutation;

import java.io.IOException;
import java.util.Arrays;
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
public class GraphGenerator {

    // an internal version number which tracks changes affecting graph generation.
    // Whenever there is a possibility that a code change will result in a different graph
    // (despite identical model, size, and seed value), this version should be changed so as to avoid
    // two distinct generated graphs sharing a name.
    static final String VERSION = "v1";

    private static Logger logger = Logger.getLogger(GraphGenerator.class.getName());

    private final GraphModel model;
    private Map<String, Long> vertexPartition;
    private long batchSize = 1000;
    private int batchCounter = 0;
    private Runnable transactionListener;

    // used in random permutations and degree distributions
    protected long randomSeed;
    private Random random;

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

    public Map<String, Long> getVertexPartition() {
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
        random = new Random(randomSeed);
    }

    /**
     * Sets a listener for commit events
     *
     * @param listener a task which is invoked each time data is committed to the graph.
     *                 By default, there is no listener.
     */
    public void setTransactionListener(final Runnable listener) {
        this.transactionListener = listener;
    }

    /**
     * Generates a graph with a given number of vertices.
     *
     * @param graph         the property graph to which generated elements are to be added
     * @param totalVertices the size of the generated graph, in terms of vertices
     */
    public synchronized void generateTo(final DB graph, final long totalVertices) throws IOException {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(totalVertices > 0);

        graph.setVocabulary(getModel().getSchemaVocabulary());

        vertexPartition = model.getVertexPartitioner().getPartitionSizes(totalVertices);

        long time = timeTask(() -> {
            createVertices(graph);
            createEdges(graph);
            commit(graph);
        });
        logger.info("generated graph of " + totalVertices + " vertices in " + time + "ms");
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
        if (null != transactionListener) {
            transactionListener.run();
        }
        graph.getMetrics().batchCommit.measure(() -> {
            graph.commitBatch();
        });
    }

    private void createVertices(final DB graph) throws IOException {
        for (Map.Entry<String, Long> e : vertexPartition.entrySet()) {
            String label = e.getKey();
            PropertyModel props = model.getVertexPropertyModels().get(label);
            long nVertices = e.getValue();
            logger.info("generating " + nVertices + " " + label + "...");
            ProgressReporter progressReporter = new ProgressReporter("gen vertex " + label, nVertices, 102400L);
            for (long i = 0; i < nVertices; i++) {
                createVertex(label, i, graph, props);
                progressReporter.maybeReport(i);
            }
            progressReporter.report(nVertices);
        }
    }

    private void createEdges(final DB graph) {
        for (Map.Entry<String, EdgeModel> e : model.getEdgeModels().entrySet()) {
            createEdges(e.getKey(), e.getValue(), graph);
        }
    }

    private void createEdges(final String edgeLabel, final EdgeModel edgeStats, final DB graph) {

        String domainLabel = edgeStats.getDomainIncidence().getVertexLabel();
        String rangeLabel = edgeStats.getRangeIncidence().getVertexLabel();

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
            new DirectSet(domainBucketCount),
            edgeStats.getDomainIncidence().getExistenceProbability());
        RandomSubset<Integer> rangeSubset = createRandomSubset(
            new DirectSet(rangeBucketCount),
            edgeStats.getRangeIncidence().getExistenceProbability());

        // domain and range distributions
        DegreeDistribution.Sample domainSample
            = edgeStats.getDomainIncidence().getDegreeDistribution().createSample(domainSubset.size(), random);
        DegreeDistribution.Sample rangeSample
            = edgeStats.getRangeIncidence().getDegreeDistribution().createSample(rangeSubset.size(), random);

        WeightedBuckets domainWeightedBuckets = new WeightedBuckets(domainSample, domainSubset.size());
        WeightedBuckets rangeWeightedBuckets = new WeightedBuckets(rangeSample, rangeSubset.size());

        long totalEdge = (long) domainWeightedBuckets.getTotalWeight() * domainBucketWidth;

        String prefix = String.format("gen %s(%d/%d):%s:%s(%d/%d)",
            domainLabel, domainExistCount, domainSize,
            edgeLabel,
            rangeLabel, rangeExistCount, rangeSize
        );

        ProgressReporter progressReporter = new ProgressReporter(prefix, totalEdge, 102400L);


        long edgeCount = 0;

        while (edgeCount < totalEdge) {

            // pick tail
            int domainBucket = domainWeightedBuckets.locate(random);
            int tbd = random.nextInt(domainBucketWidth);
            long tailIndex = (long) domainSubset.get(domainBucket) * domainBucketWidth + tbd;

            // pick head
            int rangeBucket = rangeWeightedBuckets.locate(random);
            int hbd = random.nextInt(domainBucketWidth);
            long headIndex = rangeSubset.get(rangeBucket) * rangeBucketWidth + hbd;


            // System.out.println(String.format("domain sample index %d %d=%d*%d+%d", t, tailIndex, domainSubset.get(t), domainBucketWidth, d));
            createEdge(edgeLabel, domainLabel, tailIndex, rangeLabel, headIndex, graph);
            edgeCount++;

            progressReporter.maybeReport(edgeCount);
        }
        progressReporter.report(totalEdge);
    }

    private void createVertex(final String label, long id, final DB graph, final PropertyModel props) {

        Object vertexId = graph.genVertexId(label, id);
        Object[] params = new Object[(props == null ? 0 : props.getProperties().size() * 2)];

        if (null != props) {
            int i = 0;
            for (SimpleProperty prop : props.getProperties()) {
                params[i] = prop.getKey();
                Object value = prop.getValueGenerator().generate(this.randomSeed, label, id, prop.getKey());
                params[i + 1] = value;
                i += 2;
            }
        }
        graph.writeVertex(label, vertexId, params);
        incrementBatchCounter(graph);
    }

    private void createEdge(final String label,
                            String tailLabel, final long tailIndex,
                            String headLabel, final long headIndex,
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

    private <T> RandomSubset<T> createRandomSubset(final IndexSet<T> base, final double probability) {
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
