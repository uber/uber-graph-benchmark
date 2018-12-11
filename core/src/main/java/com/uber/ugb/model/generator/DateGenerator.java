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

public class DateGenerator extends Generator<String> {

    int min;
    int range;

    public DateGenerator(int minYear, int maxYear) {
        this.min = minYear;
        this.range = maxYear - min;
    }

    @Override
    protected String genValue() {
        int year = random.nextInt(range);
        int month = random.nextInt(12);
        int day = random.nextInt(30);
        return String.format("%4d-%2d-%2d", year + min, month + 1, day + 1);
    }
}
