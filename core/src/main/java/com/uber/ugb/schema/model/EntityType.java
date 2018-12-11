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

import com.uber.ugb.schema.InvalidSchemaException;
import com.uber.ugb.schema.Vocabulary;

import java.util.HashSet;
import java.util.Set;

/**
 * A type or class of things of interest in a domain.
 * For example, a <code>Person</code> type might be used to represent the class of all things that are people.
 * An <code>EntityType</code> is associated with <code>RelationType</code>s that relate it to other types.
 * For example, the relation <code>referredBy</code> might be used to represent the relationship between two people,
 * one of whom has referred the other to sign up for a service.
 * All types (other than a handful of base types defined in the "core" schema) must extend at least one other type.
 * Simple datatypes have no outgoing links but may contain enumerated values
 * (such as "Sunday", "Monday", etc. for a DayOfWeek type),
 * while more complex types can have both outgoing and incoming relations.
 */
public class EntityType extends Type<EntityType> {
    private static final long serialVersionUID = 7960427126481962401L;

    private Object[] values;
    private boolean isDataType;

    /**
     * Constructs a new entity type. Normally, you should not need to call this method, as types are built up
     * from specs defined in YAML.
     */
    public EntityType(final Schema schema, final String name) {
        super(schema, name);
    }

    /**
     * Finds the base datatype of this entity type, if any, returning its internal name
     *
     * @return the base datatype of the given entityType, if any
     */
    public EntityType getSimpleBaseType() throws InvalidSchemaException {
        String name = getLabel();
        EntityType baseType = null;
        for (EntityType e : getBaseTypes()) {
            if (null != baseType) {
                throw new InvalidSchemaException("datatype " + name
                        + " extends more than one base entity type");
            } else {
                baseType = e;
            }
        }

        try {
            Vocabulary.BasicType.valueOf(baseType.getLabel());
            isDataType = true;
        } catch (Exception e) {
            // ignore; forgivable use of exceptions for control flow
        }

        return baseType;
    }

    /**
     * Gets the enumerated values of this entity type, if any
     */
    public Object[] getValues() {
        return getField(type -> type.values);
    }

    /**
     * Sets the enumerated values of this entity type
     */
    public void setValues(Object[] values) {
        this.values = values;
    }

    /**
     * Determines whether this entity type is a basic data type (e.g. String, Boolean, or an enumerated type)
     */
    public boolean getIsDataType() {
        return isDataType;
    }

    private Set<EntityType> getBaseTypes() throws InvalidSchemaException {
        Set<EntityType> entities = new HashSet<>();
        addBaseDataTypes(entities);
        return entities;
    }

    @Override
    public int hashCode() {
        return label.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof EntityType && ((EntityType) other).getLabel().equals(label);
    }

    private void addBaseDataTypes(final Set<EntityType> baseEntities)
            throws InvalidSchemaException {

        if (null == getExtends() || 0 == getExtends().size()) {
            baseEntities.add(this);
        } else {
            for (EntityType ent : getExtends()) {
                ent.addBaseDataTypes(baseEntities);
            }
        }
    }
}
