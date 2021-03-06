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

import com.google.common.base.Objects;
import jline.internal.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * A collection of entity and relation type definitions describing a domain of interest.
 * A schema has a name, and may depend on other schemas.
 * It also includes any number of "indexes" on relations, as a hint to the storage layer.
 */
public class Schema extends SchemaElement {
    private static final long serialVersionUID = 8935102833914197707L;

    protected String name;
    private List<Schema> includes;
    private Map<String, EntityType> entityTypes;
    private Map<String, RelationType> relationTypes;
    private List<Index> indexes;

    /**
     * Constructs a schema with the given unique name
     */
    public Schema(final String name) {
        this.name = name;
    }

    /**
     * Gets the unique name of the schema
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the unique name of the schema
     */
    public void setName(String name) {
        Preconditions.checkNotNull(name);
        this.name = name;
    }

    /**
     * Gets the dependencies ("includes") of the schema
     */
    public List<Schema> getIncludes() {
        return includes;
    }

    /**
     * Sets the dependencies ("includes") of the schema
     */
    public void setIncludes(List<Schema> includes) {
        this.includes = includes;
    }

    /**
     * Gets a map of all entity types in the schema, by label
     */
    public Map<String, EntityType> getEntityTypes() {
        return entityTypes;
    }

    /**
     * Sets the map of all entity types in the schema, by label
     */
    public void setEntityTypes(Map<String, EntityType> entityTypes) {
        this.entityTypes = entityTypes;
    }

    /**
     * Gets a map of all relation types in the schema, by label
     */
    public Map<String, RelationType> getRelationTypes() {
        return relationTypes;
    }

    /**
     * Sets the map of all relation types in the schema, by label
     */
    public void setRelationTypes(Map<String, RelationType> relationTypes) {
        this.relationTypes = relationTypes;
    }

    /**
     * Gets a list of all index hints in the schema
     */
    public List<Index> getIndexes() {
        return indexes;
    }

    /**
     * Sets the list of all index hints in the schema
     */
    public void setIndexes(List<Index> indexes) {
        this.indexes = indexes;
    }

    @Override
    public boolean equals(final Object that) {
        return that instanceof Schema && Objects.equal(this.getName(), ((Schema) that).getName());
    }

    @Override
    public int hashCode() {
        return hashCodeOf(name);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getName() + "]";
    }

    private int hashCodeOf(final String s) {
        return null == s ? 0 : s.hashCode();
    }
}
