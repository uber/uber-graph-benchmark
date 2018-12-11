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

package com.uber.ugb.model.generator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class EmailGeneratorTest {

    @Test
    public void testEmailGen() {
        EmailGenerator emailGenerator = new EmailGenerator();

        String x = (String) emailGenerator.generate(1, "User", 1, "email");
        String y = (String) emailGenerator.generate(1, "User", 1, "email");
        String z = (String) emailGenerator.generate(1, "User", 1, "emailAddress");

        assertEquals(x, y);
        assertNotEquals(x, z);

        String xx = (String) emailGenerator.generate(2, "User", 1, "email");
        assertNotEquals(x, xx);

        System.out.println("email:"+x);
        System.out.println("email:"+z);
        System.out.println("email:"+xx);

    }
}
