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
import com.uber.ugb.schema.QualifiedName;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;

/**
 * An entity or relation type belonging to a schema
 *
 * @param <E> either <code>EntityType</code> or <code>RelationType</code>
 */

public abstract class Type<E extends Type> extends SchemaElement {
    private static final long serialVersionUID = -8161344924833291717L;

    protected String label;
    private boolean isAbstract;
    private String sameAs;
    private List<E> extended;

    private Schema belongsTo;

    protected Type(final Schema belongsTo, final String label) {
        this.belongsTo = belongsTo;
        this.label = label;
    }

    public Schema getBelongsTo() {
        return belongsTo;
    }

    public String getLabel() {
        return label;
    }

    public List<E> getExtends() {
        return extended;
    }

    public void setExtends(List<E> extended) {
        this.extended = extended;
    }

    public QualifiedName getName() {
        return new QualifiedName(belongsTo.getName(), getLabel());
    }

    public boolean getAbstract() {
        return isAbstract;
    }

    public void setAbstract(boolean isAbstract) {
        this.isAbstract = isAbstract;
    }

    public String getSameAs() {
        return sameAs;
    }

    public void setSameAs(String sameAs) {
        this.sameAs = sameAs;
    }

    protected <T> T getField(final Function<E, T> accessor) {

        T value = accessor.apply((E) this);

        if (null == value) {
            Collection<E> extended = this.getExtends();
            if (null != extended) {
                for (E parent : extended) {
                    T parentValue = (T) parent.getField(accessor);
                    if (null != parentValue) {
                        if (null != value) {
                            if (!parentValue.equals(value)) {
                                throw new InvalidSchemaException("incompatible multiple inheritance: "
                                        + value + " vs " + parentValue);
                            }
                        }
                        value = parentValue;
                    }
                }
            }
        }

        return value;
    }

    public Set<E> getInferredTypes() {
        Set<E> inferredTypes = new HashSet<>();
        Stack<E> types = new Stack<E>();
        types.push((E) this);
        while (!types.isEmpty()) {
            E type = types.pop();
            inferredTypes.add(type);
            for (E ext : (List<E>) type.getExtends()) {
                types.push(ext);
            }
        }
        return inferredTypes;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getName() + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), getName());
    }

    @Override
    public boolean equals(final Object other) {
        return other.getClass().equals(getClass())
                && ((Type) other).getName().equals(getName());
    }
}
