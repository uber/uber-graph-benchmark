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

import java.io.Serializable;
import java.util.Random;

public abstract class Generator<V> implements Serializable {

    protected Random random = new Random();

    protected abstract V genValue();

    public Object generate(long randomSeed, String label, long id, String key) {
        long seed = randomSeed;
        seed = ((seed << 5) - seed) + id;
        seed = ((seed << 5) - seed) + label.hashCode();
        seed = ((seed << 5) - seed) + ".".hashCode();
        seed = ((seed << 5) - seed) + key.hashCode();
        this.random.setSeed(seed);
        return genValue();
    }

}
