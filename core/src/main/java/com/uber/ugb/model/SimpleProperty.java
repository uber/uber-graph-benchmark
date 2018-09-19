package com.uber.ugb.model;

import com.uber.ugb.model.generator.Generator;
import com.uber.ugb.schema.model.RelationType;

import java.io.Serializable;

public class SimpleProperty<T> implements Serializable {
    private static final long serialVersionUID = -209172946806510138L;

    private final RelationType relationType;
    private final Generator valueGenerator;

    public SimpleProperty(final RelationType relationType, Generator valueGenerator) {
        this.relationType = relationType;
        this.valueGenerator = valueGenerator;
    }

    public RelationType getRelationType() {
        return relationType;
    }

    public String getKey() {
        return relationType.getLabel();
    }

    public Generator getValueGenerator() {
        return valueGenerator;
    }
}
