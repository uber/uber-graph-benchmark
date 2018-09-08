package com.uber.ugsb.measurement;

import com.google.gson.JsonObject;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class LatencyHistogramTest {

    @Test
    public void testJsonOutput() throws IOException {
        LatencyHistogram mm = new LatencyHistogram("write");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonMetricsOutput export = new JsonMetricsOutput();

        int minNs = 800;
        int maxNs = 10000;
        for (int i = minNs; i < maxNs; i++) {
            mm.measure(i);
        }
        minNs = 500000000;
        maxNs = 601000000;
        for (int i = minNs; i < maxNs; i++) {
            mm.measure(i);
        }

        minNs = 650000000;
        maxNs = 652000000;
        for (int i = minNs; i < maxNs; i++) {
            mm.measure(i);
        }

        mm.printout(export);

        JsonObject category = export.getJson().getAsJsonObject("write");
        assertEquals(category.get("Operations").getAsString(), "103009200");
        assertEquals(category.get("Min(us)").getAsString(), "0.8");
        assertEquals(category.get("Max(us)").getAsString(), "651999.999");
        assertEquals(category.get("95thPercentile(ms)").getAsString(), "597");
        assertEquals(category.get("99thPercentile(ms)").getAsString(), "650");
    }
}
