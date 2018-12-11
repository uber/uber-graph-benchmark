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

import java.math.BigDecimal;

public class DecimalGenerator extends Generator<BigDecimal> {

    double min;
    double max;
    double range;

    public DecimalGenerator(double min, double max){
        this.min = min;
        this.max = max;
        this.range = max - min;
    }

    @Override
    protected BigDecimal genValue() {
        return new BigDecimal(Math.abs(random.nextDouble()*this.range)+this.min);
    }
}
