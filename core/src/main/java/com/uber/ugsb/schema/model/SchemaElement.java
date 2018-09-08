package com.uber.ugsb.schema.model;

import com.uber.ugsb.schema.Vocabulary;

import java.io.Serializable;

/**
 * Any object used in a schema definition, including the schema itself, its types, and its index hints.
 * Schema elements may have natural-language descriptions and/or comments.
 */
public abstract class SchemaElement implements Serializable {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

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
