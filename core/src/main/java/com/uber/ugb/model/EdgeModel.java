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

import java.io.Serializable;

public class EdgeModel implements Serializable {
    private static final long serialVersionUID = 5511473326817973109L;

    private final Incidence domainIncidence;
    private final Incidence rangeIncidence;

    public EdgeModel(final Incidence domainIncidence,
              final Incidence rangeIncidence) {
        this.domainIncidence = domainIncidence;
        this.rangeIncidence = rangeIncidence;
    }

    public Incidence getDomainIncidence() {
        return domainIncidence;
    }

    public Incidence getRangeIncidence() {
        return rangeIncidence;
    }
}
