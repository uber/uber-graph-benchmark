package com.uber.ugb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.uber.ugb.db.DB;
import com.uber.ugb.db.NoopDB;
import com.uber.ugb.db.ParallelWriteDBWrapper;
import com.uber.ugb.model.GraphModel;
import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class Benchmark {

    public static final String WRITE_VERTEX_COUNT_PROPERTY = "write.vertex.count";
    public static final String WRITE_SEED_PROPERTY = "write.seed";
    public static final String WRITE_THREAD_COUNT_PROPERTY = "write.thread.count";
    public static final String READ_THREAD_COUNT_PROPERTY = "read.thread.count";
    public static final String READ_OPERATION_COUNT_PROPERTY = "read.operation.count";
    public static Logger logger = Logger.getLogger(Benchmark.class.getName());

    public static void main(String[] args) {
        // create the command line parser
        CommandLineParser parser = new BasicParser();

        // create the Options
        Options options = new Options();
        options.addOption("r", "read", false, "benchmark the reads");
        options.addOption("w", "write", false, "benchmark the writes");
        options.addOption("db", "db", true, "the class name for the graph db");
        options.addOption("g", "graph", true,
            "the folder containing all the graph definitions with the schema and distribution yaml files");
        options.addOption("b", "benchload", true,
            "the workload file name describing the graph to generate and the data to read");
        options.addOption("s", "spark", false, "generate data via spark");

        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            String dbname = line.getOptionValue("db", NoopDB.class.getCanonicalName());
            String graphDir = line.getOptionValue("g", "benchdata/graphs/trips");
            boolean hasRead = line.hasOption("r");
            boolean hasWrite = line.hasOption("w");
            String workloadFile = line.getOptionValue("b", "benchdata/workloads/workloada");
            boolean isSpark = line.hasOption("s");

            System.out.println("-db=" + dbname);
            System.out.println("-g=" + graphDir);
            System.out.println("-b=" + workloadFile);
            if (hasRead) {
                System.out.println("-r");
            }
            if (hasWrite) {
                System.out.println("-w");
            }

            Properties prop = collectProperties(workloadFile);

            long operationCount = Long.valueOf(prop.getProperty(READ_OPERATION_COUNT_PROPERTY, "1"));
            int writeConcurrency = Integer.valueOf(prop.getProperty(WRITE_THREAD_COUNT_PROPERTY, "16"));
            int readConcurrency = Integer.valueOf(prop.getProperty(READ_THREAD_COUNT_PROPERTY, "16"));
            long totalVertices = Long.valueOf(prop.getProperty(WRITE_VERTEX_COUNT_PROPERTY, "0"));
            int seed = Integer.valueOf(prop.getProperty(WRITE_SEED_PROPERTY, "12345"));

            System.out.println(READ_OPERATION_COUNT_PROPERTY + "=" + operationCount);
            System.out.println(WRITE_THREAD_COUNT_PROPERTY + "=" + writeConcurrency);
            System.out.println(READ_THREAD_COUNT_PROPERTY + "=" + readConcurrency);
            System.out.println(WRITE_VERTEX_COUNT_PROPERTY + "=" + totalVertices);
            System.out.println(WRITE_SEED_PROPERTY + "=" + seed);

            // start generator
            GraphModelBuilder graphModelBuilder = new GraphModelBuilder();
            graphModelBuilder.addConceptDirectory(new File(graphDir, "concepts"));
            graphModelBuilder.setStatistics(new File(graphDir, "statistics.yaml"));
            GraphModel model = graphModelBuilder.build();
            GraphGenerator gen = new GraphGenerator(model);
            gen.setRandomSeed(seed);

            // load the db object from db class name
            DB db = loadDbFromClassName(dbname);
            // set the properties
            db.setProperties(prop);
            db.setVocabulary(model.getSchemaVocabulary());
            // init the db instance
            db.init();

            try {

                if (hasWrite) {

                    ParallelWriteDBWrapper pdb = new ParallelWriteDBWrapper(db, writeConcurrency);
                    pdb.startup();

                    if (!isSpark) {

                        gen.generateTo(pdb, totalVertices);

                    } else {

                        SparkConf sparkConf = new SparkConf(true).setAppName("UberGraphBenchmark Generator");
                        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
                        gen.generateTo(javaSparkContext, pdb, totalVertices);

                    }

                    pdb.shutdown();

                    logger.info("write done");

                }

                if (hasRead) {

                    String queriesPath = graphDir + "/queries.yaml";

                    benchmarkQueries(gen, seed, totalVertices, db, queriesPath, operationCount, readConcurrency);

                    logger.info("read done");

                }
            } finally {

                db.getMetrics().printOut(System.out);

                db.cleanup();

            }

        } catch (Exception exp) {
            logger.info("Unexpected exception:" + exp.getMessage());
            exp.printStackTrace();
        }

        logger.info("exiting...");
        System.exit(0);
    }

    private static void benchmarkQueries(GraphGenerator gen, int seed, long totalVertices, DB db, String queriesPath,
                                         long operationCount, int concurrency) throws Exception {

        Path path = Paths.get(queriesPath);
        InputStream yamlInput = new FileInputStream(new File(path.toString()));

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        QueriesSpec queriesSpec = objectMapper.readValue(yamlInput, QueriesSpec.class);

        for (QueriesSpec.Query query : queriesSpec.queries) {
            logger.info("querying " + query.name + "...");
            Map<QualifiedName, Long> vertexPartitioner =
                gen.getModel().getVertexPartitioner().getPartitionSizes(totalVertices);
            Long startVertexSetSize = vertexPartitioner.get(new QualifiedName(query.startVertexLabel));
            GraphScraper graphScraper = new GraphScraper();
            graphScraper.scrape(db, seed, startVertexSetSize, query, operationCount, concurrency);
        }

    }

    private static Properties readProperties(String fileName) {
        Properties prop = new Properties();
        InputStream input = null;

        try {

            input = new FileInputStream(fileName);

            // load a properties file
            prop.load(input);

            Enumeration<?> e = prop.propertyNames();
            while (e.hasMoreElements()) {
                String key = (String) e.nextElement();
                String value = prop.getProperty(key);
                logger.info(key + " = " + value);
            }

        } catch (IOException ex) {
            ex.printStackTrace();
            logger.info("Failed to read file:" + fileName);
            File currentDirectory = new File(new File(".").getAbsolutePath());
            logger.info(currentDirectory.getAbsolutePath());
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.info("Failed to read property file:" + fileName);
                }
            }
        }

        return prop;

    }

    private static Properties collectProperties(String workloadFilePath) {
        Properties prop = readProperties(workloadFilePath);
        File envPropFile = new File(new File(workloadFilePath).getParentFile(), "env.properties");
        if (envPropFile.exists()) {
            Properties envProp = readProperties(envPropFile.getAbsolutePath());
            for (String k : envProp.stringPropertyNames()) {
                prop.setProperty(k, envProp.getProperty(k));
            }
        }
        Map<String, String> env = System.getenv();
        prop.putAll(env);
        return prop;
    }

    private static DB loadDbFromClassName(String dbname) throws Exception {

        ClassLoader classLoader = Benchmark.class.getClassLoader();

        DB ret;

        try {
            Class dbclass = classLoader.loadClass(dbname);

            ret = (DB) dbclass.newInstance();

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("unknownDB:" + dbname, e);
        }

        return ret;
    }

}
