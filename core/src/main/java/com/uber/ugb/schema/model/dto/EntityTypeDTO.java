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

/**
 * A lightweight object representing an entity type. These DTOs are used for schema construction and serialization.
 * See <code>EntityType</code> for the materialized form of this class.
 */
public class EntityTypeDTO extends TypeDTO {
    private static final long serialVersionUID = -5286344557980177193L;

    private Object[] values;

    public EntityTypeDTO() {
    }

    public EntityTypeDTO(final String label) {
        super(label);
    }

    public Object[] getValues() {
        return values;
    }

    public void setValues(Object[] values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "EntityType[" + getLabel() + "]";
    }
}
