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

public class FloatGenerator extends Generator<Float> {

    float min;
    float max;
    float range;

    public FloatGenerator(float min, float max) {
        this.min = min;
        this.max = max;
        this.range = max - min;
    }

    @Override
    protected Float genValue() {
        return Math.abs(random.nextFloat() * this.range) + this.min;
    }
}
