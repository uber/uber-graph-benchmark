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

package com.uber.ugb.db.hbase;

import com.uber.ugb.db.PrefixKeyValueDB;
import com.uber.ugb.storage.PrefixKeyValueStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class HBaseDB extends PrefixKeyValueDB {

    private static Logger logger = LoggerFactory.getLogger(HBaseDB.class);

    transient HBaseStore hBaseStore;

    public HBaseDB() {
        super();
    }

    @Override
    public void init() {
        hBaseStore = new HBaseStore(getProperties());
        setPrefixKeyValueStore(hBaseStore);
    }

    @Override
    public void cleanup() {
        hBaseStore.shutdown();
    }

    public static class HBaseStore implements PrefixKeyValueStore {

        private static byte[] cf = "cf1".getBytes();
        private static byte[] props = "p".getBytes();

        Configuration config = HBaseConfiguration.create();
        Connection conn;
        private String vertexTableName;
        private String edgeTableName;

        public HBaseStore(Properties properties) {
            String zookeeperQuorum = properties.getProperty("hbase.zookeeper.quorum", "");
            String zookeeperZnodeParent =
                    properties.getProperty("hbase.zookeeper.znode.parent", "/hbase");
            String zookeeperClientPort =
                    properties.getProperty("hbase.zookeeper.znode.property.clientPort", "2181");
            try {
                config.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
                config.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.parseInt(zookeeperClientPort));
                config.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zookeeperZnodeParent);
                conn = ConnectionFactory.createConnection(config);
                vertexTableName = properties.getProperty("hbase.vertexTableName", "vertex");
                edgeTableName = properties.getProperty("hbase.edgeTableName", "edge");

                ensureVertexTable();
                ensureEdgeTable();

            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        private void ensureVertexTable() throws IOException {
            ensureTable(vertexTableName);
            logger.info("Ensure vertex table {} is created.", vertexTableName);
        }

        private void ensureTable(String name) throws IOException {
            TableName tableName = TableName.valueOf(name);
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor(cf));
            if (!conn.getAdmin().tableExists(tableName)) {
                conn.getAdmin().createTable(tableDescriptor);
            }
        }

        private void ensureEdgeTable() throws IOException {
            ensureTable(edgeTableName);

            logger.info("Ensure edge table {} is created.", edgeTableName);
        }

        @Override
        public List<PrefixQueriedRow> scan(byte[] prefix, int limit) {

            List<PrefixQueriedRow> rows = new ArrayList<>();

            TableName tablename = TableName.valueOf(edgeTableName);

            try (Table table = conn.getTable(tablename)) {
                Scan scan = new Scan();
                scan.setRowPrefixFilter(prefix);

                ResultScanner scanner = table.getScanner(scan);
                for (Result result : scanner) {
                    byte[] key = result.getRow();
                    if (!Bytes.equals(
                            key, 0, prefix.length,
                            prefix, 0, prefix.length)) {
                        continue;
                    }
                    ByteBuffer byteBufferToVertexId = ByteBuffer.wrap(
                            key, prefix.length, key.length - prefix.length);
                    byte[] value = result.getValue(cf, props);

                    rows.add(new PrefixQueriedRow(
                            key, prefix.length, key.length - prefix.length,
                            value, 0, value.length
                    ));
                }
                scanner.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

            return rows;

        }

        @Override
        public void put(byte[] keyPrefix, byte[] keySuffix, byte[] value) {

            TableName tablename = TableName.valueOf(edgeTableName);

            try (Table table = conn.getTable(tablename)) {

                byte[] key = new byte[keyPrefix.length + keySuffix.length];
                System.arraycopy(keyPrefix, 0, key, 0, keyPrefix.length);
                System.arraycopy(keySuffix, 0, key, keyPrefix.length, keySuffix.length);

                Put put = new Put(key);
                put.addColumn(cf, props, value);

                table.put(put);

            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        @Override
        public byte[] get(byte[] key) {

            TableName tablename = TableName.valueOf(vertexTableName);

            try (Table table = conn.getTable(tablename)) {
                Get get = new Get(key).addColumn(cf, props);
                Result result = table.get(get);

                byte[] value = result.getValue(cf, props);

                return value;

            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }

        @Override
        public void put(byte[] key, byte[] value) {

            TableName tablename = TableName.valueOf(vertexTableName);

            try (Table table = conn.getTable(tablename)) {

                Put put = new Put(key);
                put.addColumn(cf, props, value);

                table.put(put);

            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        public void shutdown() {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
