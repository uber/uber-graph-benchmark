package com.uber.ugsb;

import com.google.common.base.Preconditions;
import com.uber.ugsb.db.DB;
import com.uber.ugsb.model.EdgeModel;
import com.uber.ugsb.model.GraphModel;
import com.uber.ugsb.model.PropertyModel;
import com.uber.ugsb.model.SimpleProperty;
import com.uber.ugsb.model.distro.DegreeDistribution;
import com.uber.ugsb.schema.model.RelationType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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
    private Map<String, IndexSet<Integer>> vertexPartition;
    private long batchSize = 1000;
    private int batchCounter = 0;
    private Runnable transactionListener;

    private Set<RelationType> csvProps;
    private File csvDir;
    private CsvOutputs csvOutputs;

    // used in random permutations and degree distributions
    private long randomSeed;
    private Random random;

    /**
     * Creates a new generator with the given statistical model
     *
     * @param model the statistical model which generated graphs are to follow
     */
    GraphGenerator(final GraphModel model) {
        this.model = model;
        setRandomSeed(new Random().nextLong());
    }

    private static String abbreviateNumber(final long number) {
        double[] powers = {1e3, 1e6, 1e9, 1e12};
        String[] suffixes = {"k", "M", "G", "T"};
        for (int i = powers.length - 1; i >= 0; i--) {
            if (number >= powers[i]) {
                double decimal = number / powers[i];
                return Double.compare(decimal, Math.floor(decimal)) == 0
                    ? (int) decimal + suffixes[i]
                    : decimal + suffixes[i];
            }
        }
        return "" + number;
    }

    void close() throws IOException {
        csvOutputs.shutDown();
    }

    public GraphModel getModel() {
        return model;
    }

    public Map<String, IndexSet<Integer>> getVertexPartition() {
        return vertexPartition;
    }

    public void setCsvProps(Set<RelationType> csvProps) {
        this.csvProps = csvProps;
    }

    public void setCsvDir(File csvDir) {
        this.csvDir = csvDir;
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
    public synchronized void generateTo(final DB graph, final int totalVertices) throws IOException {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(totalVertices > 0);
        // verifyGraphIsEmpty(graph); // this does not work since Ugsf does not support fetching all vertices.

        createCsvOutputs();
        graph.setVocabulary(getModel().getSchemaVocabulary());

        long time = timeTask(() -> {
            createVertices(graph, totalVertices);
            createEdges(graph);
            commit(graph);
        });
        logger.info("generated graph of " + totalVertices + " vertices in " + time + "ms");
    }

    private void createCsvOutputs() throws IOException {
        if (null != csvDir && null != csvProps && csvProps.size() > 0) {
            csvOutputs = new CsvOutputs(csvProps, csvDir);
        }
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

    private void createVertices(final DB graph, final int totalVertices) throws IOException {
        vertexPartition = model.getVertexPartitioner().getPartition(totalVertices);
        for (Map.Entry<String, IndexSet<Integer>> e
            : vertexPartition.entrySet()) {
            String label = e.getKey();
            PropertyModel props = model.getVertexPropertyModels().get(label);
            Integer nVertices = e.getValue().size();
            logger.info("generating " + nVertices + " " + label + "...");
            for (int i = 0; i < nVertices; i++) {
                createVertex(label, i, graph, props);
            }
        }
    }

    private void addProperties(final Vertex vertex, final PropertyModel props) throws IOException {
        if (null != props) {
            for (SimpleProperty prop : props.getProperties()) {
                addProperty(vertex, prop);
            }
        }
    }

    private void addProperty(final Vertex vertex, final SimpleProperty prop) throws IOException {
        Object value = prop.getValueGenerator().apply(random);
        vertex.property(prop.getKey(), value);

        if (null != csvOutputs) {
            csvOutputs.handleProperty(vertex, prop.getRelationType(), value);
        }
    }

    private void createEdges(final DB graph) {
        for (Map.Entry<String, EdgeModel> e : model.getEdgeModels().entrySet()) {
            createEdges(e.getKey(), e.getValue(), graph);
        }
    }

    private void createEdges(final String edgeLabel, final EdgeModel edgeStats, final DB graph) {
        String domainLabel = edgeStats.getDomainIncidence().getVertexLabel();
        String rangeLabel = edgeStats.getRandeIncidence().getVertexLabel();

        IndexSet<Integer> domainSet = vertexPartition.get(domainLabel);
        IndexSet<Integer> rangeSet = vertexPartition.get(rangeLabel);

        // subset of domain vertices and range vertices which will have at least one edge of the given label
        RandomSubset<Integer> domainSubset = createRandomSubset(
            new DirectSet(domainSet.size()),
            edgeStats.getDomainIncidence().getExistenceProbability());
        RandomSubset<Integer> rangeSubset = createRandomSubset(
            new DirectSet(rangeSet.size()),
            edgeStats.getRandeIncidence().getExistenceProbability());

        // domain and range distributions
        DegreeDistribution.Sample domainSample
            = edgeStats.getDomainIncidence().getDegreeDistribution().createSample(domainSet.size(), random);
        DegreeDistribution.Sample rangeSample
            = edgeStats.getRandeIncidence().getDegreeDistribution().createSample(rangeSubset.size(), random);

        logger.info("generating edges: from " + domainLabel + " to " + rangeLabel + "...");
        for (int i = 0; i < domainSubset.size; i++) {
            int tailIndex = domainSubset.get(i);
            // TODO: account for degree == 0
            int outDegree = domainSample.getNextDegree();
            for (int j = 0; j < outDegree; j++) {
                // TODO: account for missing indexes
                int k = rangeSample.getNextIndex();
                // continue only if we have not exhausted the range subset
                // We can't necessarily re-use range entities, e.g. if the relationship is OneToMany
                if (k < rangeSubset.size) {
                    int headIndex = rangeSubset.get(k);
                    createEdge(edgeLabel, domainLabel, tailIndex, rangeLabel, headIndex, graph);
                }
            }
        }
    }

    private void createVertex(final String label, long id, final DB graph, final PropertyModel props) {

        Object vertexId = graph.genVertexId(label, id);
        Object[] params = new Object[(props == null ? 0 : props.getProperties().size() * 2)];

        if (null != props) {
            int i = 0;
            for (SimpleProperty prop : props.getProperties()) {
                params[i] = prop.getKey();
                Object value = prop.getValueGenerator().apply(random);
                params[i + 1] = value;
                i += 2;
            }
        }
        graph.getMetrics().writeVertex.measure(() -> {
            graph.writeVertex(label, vertexId, params);
        });
        incrementBatchCounter(graph);
    }

    private void createEdge(final String label,
                            String tailLabel, final int tailIndex,
                            String headLabel, final int headIndex,
                            final DB graph) {
        Object tailId = graph.genVertexId(tailLabel, tailIndex);
        Object headId = graph.genVertexId(headLabel, headIndex);

        graph.getMetrics().writeEdge.measure(() -> {
            graph.writeEdge(label, tailLabel, tailId, headLabel, headId);
        });

        incrementBatchCounter(graph);
    }

    private <T> RandomSubset<T> createRandomSubset(final IndexSet<T> base, final double probability) {
        int size = (int) (probability * base.size());
        return new RandomSubset<>(base, size, random);
    }

    public String keyspaceForDataset(final long size) {
        String simple = "" + size;
        String abbrev = abbreviateNumber(size);
        String sizeStr = (simple.length() <= abbrev.length()
            ? simple
            : abbrev.replaceAll("\\.", "_"));
        return "gen" + sizeStr + "_" + VERSION + "_" + model.getHash() + "_" + randomSeed;
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

    private static class CsvOutputs {

        private Map<RelationType, OutputStream> streamsByProperty;

        CsvOutputs(final Set<RelationType> props, final File dir) throws FileNotFoundException {
            streamsByProperty = new HashMap<>();
            for (RelationType prop : props) {
                streamsByProperty.put(prop, streamForProperty(dir, prop));
            }
        }

        void shutDown() throws IOException {
            for (OutputStream stream : streamsByProperty.values()) {
                stream.close();
            }
        }

        void handleProperty(final Vertex vertex, final RelationType prop, final Object value) throws IOException {
            OutputStream out = streamsByProperty.get(prop);

            out.write(vertex.id().toString().getBytes());
            out.write(',');
            out.write(vertex.label().getBytes());
            out.write(',');
            out.write(value.toString().getBytes());
            out.write('\n');
        }

        private FileOutputStream streamForProperty(final File dir, final RelationType property)
            throws FileNotFoundException {
            File file = new File(dir, property.getName() + ".csv");
            return new FileOutputStream(file);
        }
    }
}
