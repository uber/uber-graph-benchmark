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
