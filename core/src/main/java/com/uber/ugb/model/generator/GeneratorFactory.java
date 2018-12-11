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

import com.uber.ugb.schema.Vocabulary;
import com.uber.ugb.schema.model.EntityType;

public class GeneratorFactory {
    public Generator make(Vocabulary vocabulary, EntityType toType) {
        switch (toType.getLabel()) {
            case "PhoneNumber":
                return new PhoneNumberGenerator();
            case "Year":
                return new YearGenerator(2011, 2050);
            case "UnixTimeMs":
                return new UnixTimeMsGenerator();
            case "CountryIso2Code":
                return new StringGenerator(2, 2);
            case "UsdAmount":
                return new DecimalGenerator(0, 200);
            case "Email":
                return new EmailGenerator();
            case "Latitude":
                return new DecimalGenerator(-90, 90);
            case "Longitude":
                return new DecimalGenerator(-180, 180);
            case "String":
                return new StringGenerator(5, 20);
            case "Boolean":
                return new BooleanGenerator();
            case "Decimal":
                return new DecimalGenerator(0, 100);
            case "Double":
                return new DoubleGenerator(0, 100);
            case "Float":
                return new FloatGenerator(0, 1000);
            case "Long":
                return new LongGenerator(0, 1000);
            case "Date":
                return new DateGenerator(1970, 2050);
        }
        if (toType.getExtends() != null) {
            for (EntityType t : toType.getExtends()) {
                Generator generator = make(vocabulary, t);
                if (generator != null) {
                    return generator;
                }
            }
        }
        return new StringGenerator(1, 10);
    }
}
