package com.uber.ugb.schema.model.dto;

import com.uber.ugb.schema.Vocabulary;
import com.uber.ugb.schema.model.Cardinality;

/**
 * A lightweight object representing an inline relationship type (one which appears directly beneath an entity type
 * in the YAML format).
 * These DTOs are used for schema construction and serialization.
 * See <code>RelationType</code> for the materialized form of this class.
 */
public class InlineRelationTypeDTO extends TypeDTO {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

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
