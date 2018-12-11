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

package com.uber.ugb.measurement;

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
