package com.uber.ugsb.schema.model.dto;

import com.uber.ugsb.schema.Vocabulary;

/**
 * A lightweight object representing an entity type. These DTOs are used for schema construction and serialization.
 * See <code>EntityType</code> for the materialized form of this class.
 */
public class EntityTypeDTO extends TypeDTO {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

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
