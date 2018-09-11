package com.uber.ugb.model;

import com.uber.ugb.schema.Vocabulary;
import com.uber.ugb.schema.model.RelationType;

import java.io.Serializable;
import java.util.Random;

public class SimpleProperty<T> implements Serializable {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

    private final RelationType relationType;
    private final SerializableFunction<Random, T> valueGenerator;

    public SimpleProperty(final RelationType relationType, SerializableFunction<Random, T> valueGenerator) {
        this.relationType = relationType;
        this.valueGenerator = valueGenerator;
    }

    public RelationType getRelationType() {
        return relationType;
    }

    public String getKey() {
        return relationType.getLabel();
    }

    public SerializableFunction<Random, T> getValueGenerator() {
        return valueGenerator;
    }
}
