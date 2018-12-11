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

package com.uber.ugb.schema.model.dto;

import com.uber.ugb.schema.model.Cardinality;

/**
 * A lightweight object representing an inline relationship type (one which appears directly beneath an entity type
 * in the YAML format).
 * These DTOs are used for schema construction and serialization.
 * See <code>RelationType</code> for the materialized form of this class.
 */
public class InlineRelationTypeDTO extends TypeDTO {
    private static final long serialVersionUID = 663532412776638958L;

    private String to;
    private Cardinality cardinality;
    private boolean required;
    private boolean requiredOf;
    private boolean unidirected;

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public Cardinality getCardinality() {
        return cardinality;
    }

    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    public boolean getRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public boolean getRequiredOf() {
        return requiredOf;
    }

    public void setRequiredOf(boolean requiredOf) {
        this.requiredOf = requiredOf;
    }

    public boolean getUnidirected() {
        return unidirected;
    }

    public void setUnidirected(boolean unidirected) {
        this.unidirected = unidirected;
    }
}
