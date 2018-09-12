package com.uber.ugb.model;

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

        Map<String, Long> partition = partitioner.getPartitionSizes(70);
        assertEquals(true, partition.get("zero") == 0);
        assertEquals(true, partition.get("one") == 10);
        assertEquals(true, partition.get("two") == 20);
        assertEquals(true, partition.get("four") == 40);
        assertNull(partition.get("five"));
    }

    @Test
    public void partitionLargeGraph() {
        Partitioner partitioner = new Partitioner();
        partitioner.put("user", 13);
        partitioner.put("trip", 8);
        partitioner.put("document", 1);

        Map<String, Long> partition = partitioner.getPartitionSizes(1000000000);

        // System.out.println("user:" + partition.get("user").size());
        // System.out.println("trip:" + partition.get("trip").size());
        // System.out.println("document:" + partition.get("document").size());

        assertEquals(true, partition.get("user") > 0);
        assertEquals(true, partition.get("trip") > 0);
        assertEquals(true, partition.get("document") > 0);
    }

}
