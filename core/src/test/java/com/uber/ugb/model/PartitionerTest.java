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

package com.uber.ugb.model;

import com.uber.ugb.schema.QualifiedName;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PartitionerTest {
    @Test
    public void partitionerIsCorrect() {
        Partitioner partitioner = new Partitioner();
        partitioner.put(new QualifiedName("zero"), 0);
        partitioner.put(new QualifiedName("one"), 1);
        partitioner.put(new QualifiedName("two"), 2);
        partitioner.put(new QualifiedName("four"), 4);

        Map<QualifiedName, Long> partition = partitioner.getPartitionSizes(70);
        assertEquals(true, partition.get(new QualifiedName("zero")) == 0);
        assertEquals(true, partition.get(new QualifiedName("one")) == 10);
        assertEquals(true, partition.get(new QualifiedName("two")) == 20);
        assertEquals(true, partition.get(new QualifiedName("four")) == 40);
        assertNull(partition.get(new QualifiedName("five")));
    }

    @Test
    public void partitionLargeGraph() {
        Partitioner partitioner = new Partitioner();
        partitioner.put(new QualifiedName("user"), 13);
        partitioner.put(new QualifiedName("trip"), 8);
        partitioner.put(new QualifiedName("document"), 1);

        Map<QualifiedName, Long> partition = partitioner.getPartitionSizes(1000000000);

        // System.out.println("user:" + partition.get("user").size());
        // System.out.println("trip:" + partition.get("trip").size());
        // System.out.println("document:" + partition.get("document").size());

        assertEquals(true, partition.get(new QualifiedName("user")) > 0);
        assertEquals(true, partition.get(new QualifiedName("trip")) > 0);
        assertEquals(true, partition.get(new QualifiedName("document")) > 0);
    }

}
