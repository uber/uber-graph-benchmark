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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class IncidenceTest {
    @Test
    public void testParsingCsv() {
        String content = "direction,degree,count\n" +
            "out,1,11\n" +
            "out,2,22\n" +
            "in,1,111\n" +
            "in,2,222\n";

        {
            List<Incidence.DegreeCount> counts = Incidence.parseCsv(content, "in");

            assertEquals(1, counts.get(0).degree);
            assertEquals(111, counts.get(0).count);
            assertEquals(2, counts.get(1).degree);
            assertEquals(222, counts.get(1).count);
        }

        {
            List<Incidence.DegreeCount> counts = Incidence.parseCsv(content, "out");

            assertEquals(1, counts.get(0).degree);
            assertEquals(11, counts.get(0).count);
            assertEquals(2, counts.get(1).degree);
            assertEquals(22, counts.get(1).count);
        }

    }
}
