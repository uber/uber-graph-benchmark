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

package com.uber.ugb.db;

import com.uber.ugb.schema.QualifiedName;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KeyValueTest {

    @Test
    public void testAppendToAdjacencyList() {

        KeyValueDB db = new KeyValueDB();

        Properties edge1Properties = new Properties();
        edge1Properties.put("name", "first");
        byte[] data = null;
        data = db.appendToAdjacencyList(data, 1L, edge1Properties);
        data = db.appendToAdjacencyList(data, 2L, null);
        Properties edge3Properties = new Properties();
        edge3Properties.put("name", "third");
        data = db.appendToAdjacencyList(data, 3L, edge3Properties);

        List<KeyValueDB.Edge> adjacencyList = db.readEdgeList(data);
        assertEquals(1L, adjacencyList.get(0).nextVertexId);
        assertEquals(2L, adjacencyList.get(1).nextVertexId);
        assertEquals(3L, adjacencyList.get(2).nextVertexId);

        assertEquals("first", adjacencyList.get(0).edgeProperties.getProperty("name"));
        assertEquals(null, adjacencyList.get(1).edgeProperties);
        assertEquals("third", adjacencyList.get(2).edgeProperties.getProperty("name"));
    }

    @Test
    public void testEdgeKeyPrefix() {

        PrefixKeyValueDB db = new PrefixKeyValueDB();

        Long startVertexId = 100L;
        Long inVertexId = 100L;
        String edgeLabel = "some_edge";

        byte[] key = db.genEdgeKeyPrefix(new QualifiedName(edgeLabel), startVertexId, inVertexId, false);
        byte[] prefix = db.genEdgeKeyPrefix(new QualifiedName(edgeLabel), startVertexId, false);

        for (int i = 0; i < prefix.length; i++) {
            assertEquals(prefix[i], key[i]);
        }

    }

}
