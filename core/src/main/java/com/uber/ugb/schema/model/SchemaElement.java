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

package com.uber.ugb.schema.model;

import java.io.Serializable;

/**
 * Any object used in a schema definition, including the schema itself, its types, and its index hints.
 * Schema elements may have natural-language descriptions and/or comments.
 */
public abstract class SchemaElement implements Serializable {
    private static final long serialVersionUID = -7126337696682103131L;

    private String description;
    private String comment;

    /**
     * Gets the human-readable description of this element
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the human-readable description of this element
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Gets any comment about this element
     */
    public String getComment() {
        return comment;
    }

    /**
     * Sets any comment for this element
     */
    public void setComment(String comment) {
        this.comment = comment;
    }
}
