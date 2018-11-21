package com.uber.ugb.db.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.uber.ugb.db.PrefixKeyValueDB;
import com.uber.ugb.storage.PrefixKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CassandraDB extends PrefixKeyValueDB {

    private static Logger logger = LoggerFactory.getLogger(CassandraDB.class);
    private static ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_ONE;

    transient CassandraStore cassandraStore;

    public CassandraDB() {
        super();
    }

    @Override
    public void init() {
        cassandraStore = new CassandraStore(getProperties());
        setPrefixKeyValueStore(cassandraStore);
    }

    @Override
    public void cleanup() {
        try {
            logger.info("Closing connection to cluster...");
            if (cassandraStore.session != null) {
                cassandraStore.session.close();
            }
            if (cassandraStore.cluster != null) {
                cassandraStore.cluster.close();
            }
        } catch (Exception e) {
            logger.error("Failed to close", e);
        } finally {
            cassandraStore.session = null;
            cassandraStore.cluster = null;
        }
    }

    public static class CassandraStore implements PrefixKeyValueStore {

        // CQL to PreparedStatement cache
        private final ConcurrentMap<String, PreparedStatement> cachedStatements = new ConcurrentHashMap<>();

        private Cluster cluster;
        private Session session;
        private String dataCenter;
        private String keyspace;
        private String vertexTableName;
        private String edgeTableName;

        public CassandraStore(Properties properties) {
            dataCenter = properties.getProperty("cassandra.dc", "");
            keyspace = properties.getProperty("cassandra.keyspace", "ugb");
            vertexTableName = properties.getProperty("cassandra.vertexTableName", "vertex");
            edgeTableName = properties.getProperty("cassandra.edgeTableName", "edge");
            String hosts = properties.getProperty("cassandra.hosts", "localhost");
            String[] contactPoints = hosts.split(",");
            int port = Integer.parseInt(properties.getProperty("cassandra.port", "9042"));
            Cluster.Builder builder = Cluster.builder();
            builder.addContactPoints(contactPoints).withPort(port);

            String username = properties.getProperty("cassandra.username", "");
            String password = properties.getProperty("cassandra.password", "");
            if (username != "" && password != "") {
                builder.withCredentials(username, password);
            }
            if (dataCenter.length() != 0) {
                builder.withLoadBalancingPolicy(
                    LatencyAwarePolicy.builder(
                        new TokenAwarePolicy(
                            DCAwareRoundRobinPolicy
                                .builder()
                                .withLocalDc(dataCenter)
                                .build())).build());
            }
            builder.withQueryOptions(new QueryOptions().setConsistencyLevel(consistencyLevel))
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withProtocolVersion(ProtocolVersion.V4);

            // pooling options
            PoolingOptions poolingOptions = new PoolingOptions();
            poolingOptions
                .setPoolTimeoutMillis(60000)
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 2)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 4)
                .setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, 4);
            builder.withPoolingOptions(poolingOptions);

            cluster = builder.build();

            Metadata metadata = cluster.getMetadata();
            logger.info("Connecting to cluster: " + metadata.getClusterName());
            for (Host host : metadata.getAllHosts()) {
                logger.info("Datacenter: " + host.getDatacenter()
                    + " Host: " + host.getAddress()
                    + " Rack: " + host.getRack());
            }
            session = cluster.connect();

            ensureKeyspace();
            ensureVertexTable();
            ensureEdgeTable();
        }

        private void ensureKeyspace() {
            String option = "with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}";
            session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s %s", keyspace, option));
            logger.info("Ensure keyspace {} is created.", keyspace);
        }

        private void ensureVertexTable() {
            String cqlPattern = "CREATE TABLE IF NOT EXISTS %s.%s("
                + "id blob, "
                + "value blob, "
                + "PRIMARY KEY (id));";

            session.execute(String.format(cqlPattern, keyspace, vertexTableName));

            logger.info("Ensure vertex table {}.{} is created.", keyspace, vertexTableName);
        }

        private void ensureEdgeTable() {
            String cqlPattern = "CREATE TABLE IF NOT EXISTS %s.%s("
                + "id1 blob, "
                + "id2 blob, "
                + "value blob, "
                + "PRIMARY KEY (id1, id2));";

            session.execute(String.format(cqlPattern, keyspace, edgeTableName));

            logger.info("Ensure edge table {}.{} is created.", keyspace, edgeTableName);
        }

        @Override
        public List<PrefixQueriedRow> scan(byte[] keyPrefix, int limit) {

            List<PrefixQueriedRow> rows = new ArrayList<>();

            String cql = String.format("SELECT id2, value FROM %s.%s WHERE id1 = ?", keyspace, edgeTableName);
            PreparedStatement preparedStatement = cachedStatements.computeIfAbsent(cql, session::prepare);
            preparedStatement.setConsistencyLevel(consistencyLevel);
            preparedStatement.setIdempotent(true);
            ResultSet resultSet = session.execute(preparedStatement.bind(ByteBuffer.wrap(keyPrefix)));

            resultSet.forEach(row -> {
                ByteBuffer byteBuffer1 = row.getBytes(0);
                if (byteBuffer1 == null) {
                    return;
                }
                ByteBuffer byteBuffer2 = row.getBytes(1);
                if (byteBuffer2 == null) {
                    return;
                }
                rows.add(new PrefixQueriedRow(byteBuffer1.array(), byteBuffer2.array()));
                return;
            });

            return rows;
        }

        @Override
        public void put(byte[] keyPrefix, byte[] keySuffix, byte[] value) {

            String cql = String.format("INSERT INTO %s.%s(id1,id2,value) VALUES(?,?,?)", keyspace, edgeTableName);
            PreparedStatement preparedStatement = cachedStatements.computeIfAbsent(cql, session::prepare);
            preparedStatement.setConsistencyLevel(consistencyLevel);
            preparedStatement.setIdempotent(true);
            session.execute(preparedStatement.bind(
                ByteBuffer.wrap(keyPrefix), ByteBuffer.wrap(keySuffix), ByteBuffer.wrap(value)));

        }

        @Override
        public byte[] get(byte[] key) {

            String cql = String.format("SELECT value FROM %s.%s WHERE id = ?", keyspace, vertexTableName);
            PreparedStatement preparedStatement = cachedStatements.computeIfAbsent(cql, session::prepare);
            preparedStatement.setConsistencyLevel(consistencyLevel);
            preparedStatement.setIdempotent(true);
            ResultSet resultSet = session.execute(preparedStatement.bind(ByteBuffer.wrap(key)));

            Row row = resultSet.one();
            if (row == null) {
                return null;
            }
            ByteBuffer byteBuffer = row.getBytes(0);
            if (byteBuffer == null) {
                return null;
            }

            return byteBuffer.array();
        }

        @Override
        public void put(byte[] key, byte[] value) {
            String cql = String.format("INSERT INTO %s.%s(id,value) VALUES(?,?)", keyspace, vertexTableName);
            PreparedStatement preparedStatement = cachedStatements.computeIfAbsent(cql, session::prepare);
            preparedStatement.setConsistencyLevel(consistencyLevel);
            preparedStatement.setIdempotent(true);
            session.execute(preparedStatement.bind(ByteBuffer.wrap(key), ByteBuffer.wrap(value)));
        }

    }
}
