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
