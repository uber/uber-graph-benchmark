package com.uber.ugb.model;

import com.uber.ugb.GraphGenerator;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PartitionerTest {
    @Test
    public void partitionerIsCorrect() {
        Partitioner partitioner = new Partitioner();
        partitioner.put("zero", 0);
        partitioner.put("one", 1);
        partitioner.put("two", 2);
        partitioner.put("four", 4);

        Map<String, GraphGenerator.IndexSet<Integer>> partition = partitioner.getPartition(70);
        assertEquals(0, partition.get("zero").size());
        assertEquals(10, partition.get("one").size());
        assertEquals(20, partition.get("two").size());
        assertEquals(40, partition.get("four").size());
        assertNull(partition.get("five"));
    }

    @Test
    public void partitionLargeGraph() {
        Partitioner partitioner = new Partitioner();
        partitioner.put("user", 13);
        partitioner.put("trip", 8);
        partitioner.put("document", 1);

        Map<String, GraphGenerator.IndexSet<Integer>> partition = partitioner.getPartition(1000000000);

        // System.out.println("user:" + partition.get("user").size());
        // System.out.println("trip:" + partition.get("trip").size());
        // System.out.println("document:" + partition.get("document").size());

        assertEquals(true, partition.get("user").size() > 0);
        assertEquals(true, partition.get("trip").size() > 0);
        assertEquals(true, partition.get("document").size() > 0);
    }

}
