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

package com.uber.ugb.schema.diff;

/**
 * A basic change to a schema, used in diffs
 */
public class SchemaChange {

    /**
     * An enumeration of all supported types of schema change
     */
    public enum Type {
        AbstractAttributeChanged,
        CardinalityChanged,
        DomainChanged,
        ExtensionAdded,
        ExtensionRemoved,
        IncludeAdded,
        IncludeRemoved,
        IndexAdded,
        IndexRemoved,
        RelationTypeAdded,
        RelationTypeRemoved,
        RangeChanged,
        RequiredAttributeChanged,
        RequiredOfAttributeChanged,
        SchemaAdded,
        SchemaRemoved,
        SchemaNameChanged,
        SchemaVersionChanged,
        EntityTypeAdded,
        EntityTypeRemoved,
    }

    private final Type type;
    private final Object[] contextPath;

    SchemaChange(final Type type, final Object... contextPath) {
        this.type = type;
        this.contextPath = contextPath;
    }

    /**
     * Gets the type of this change
     */
    public Type getType() {
        return type;
    }

    /**
     * Gets the context path of this change, which allows the changed element to be located.
     * For example, the context path of a cardinality change to a relation type called "knows" in the schema "social"
     * is <code>(Schema[social] > RelationType[knows])</code>.
     */
    public Object[] getContextPath() {
        return contextPath;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append(type.name()).append(": ");
        boolean first = true;
        for (Object p : contextPath) {
            if (first) {
                first = false;
            } else {
                sb.append(" > ");
            }
            sb.append(p);
        }
        return sb.toString();
    }
}
