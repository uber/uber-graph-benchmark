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
 * A lightweight object representing a relation type. These DTOs are used for schema construction and serialization.
 * See <code>RelationType</code> for the materialized form of this class.
 */
public class RelationTypeDTO extends InlineRelationTypeDTO {
    private static final long serialVersionUID = -8897389954512862620L;

    private String from;

    public RelationTypeDTO() {
    }

    public RelationTypeDTO(final String label) {
        this.setLabel(label);
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }
}
