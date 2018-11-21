package com.uber.ugb.measurement;

import com.google.gson.JsonObject;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class MetricsTest {
    @Test
    public void testMerge() throws IOException {
        Metrics m = new Metrics();
        Metrics m2 = new Metrics();

        m2.writeVertex.measure(1L);
        m.merge(m2);

        JsonMetricsOutput export = new JsonMetricsOutput();
        m.writeVertex.printout(export);

        JsonObject category = export.getJson().getAsJsonObject("write.vertex");
        assertEquals(category.get("Operations").getAsString(), "1");
        assertEquals(category.get("Min(us)").getAsString(), "0.001");
        assertEquals(category.get("Max(us)").getAsString(), "0.001");

    }
}
