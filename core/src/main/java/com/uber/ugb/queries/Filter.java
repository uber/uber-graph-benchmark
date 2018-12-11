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

package com.uber.ugb.queries;

public class Filter {
    public final String field;
    public final Operator operator;
    public final String value;

    public Filter(String field, Operator operator, String value) {
        this.field = field;
        this.operator = operator;
        this.value = value;
    }

    public Object getValueObject() {
        Object valueObject = null;
        // support either single quoted string or integers
        if (this.value.startsWith("'") && this.value.endsWith("'")) {
            valueObject = this.value.substring(1, this.value.length() - 1);
        } else {
            valueObject = Integer.parseInt(this.value);
        }
        return valueObject;
    }

    public enum Operator {
        GreaterThan, Equal, LessThan
    }
}
