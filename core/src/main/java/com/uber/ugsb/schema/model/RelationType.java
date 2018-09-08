package com.uber.ugsb.schema.model;

import com.uber.ugsb.schema.InvalidSchemaException;
import com.uber.ugsb.schema.Vocabulary;

/**
 * An abstract relationship between entities. A relation has a domain and a range, as well as a number of
 * implicit or explicit constraints including cardinality and requiredness.
 * As with entity types, link types may extend other types.
 */
public class RelationType extends Type<RelationType> {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

    private Type from;
    private Type to;
    private Cardinality cardinality;
    private boolean isRequired;
    private boolean isRequiredOf;
    private boolean unidirected;

    /**
     * Constructs a relation type with the given name, attached to the given schema
     */
    public RelationType(final Schema schema, final String name) {
        super(schema, name);
    }

    /**
     * @return the provided or inherited domain ("from" type) of this relation type
     */
    public Type getFromInferred() throws InvalidSchemaException {
        return getField(RelationType::getFrom);
    }

    /**
     * @return the provided or inherited range ("to" type) of this relation type
     */
    public Type getToInferred() throws InvalidSchemaException {
        return getField(RelationType::getTo);
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RelationType && ((RelationType) other).getName().equals(getName());
    }

    /**
     * Gets the requiredness of the relation type. A relation type is required if, for any given object in the domain,
     * there is a relation of this type to an object in the range.
     */
    public boolean getRequired() {
        return getField(link -> link.isRequired);
    }

    /**
     * Gets the requiredness of the relation type.
     */
    public void setRequired(boolean required) {
        this.isRequired = required;
    }

    /**
     * Gets the inverse requiredness of the relation type. A relation type is "required of" if,
     * for any given object in the range, there is a relation of this type to an object in the domain.
     */
    public boolean getRequiredOf() {
        return getField(link -> link.isRequiredOf);
    }

    /**
     * Gets the inverse requiredness of the relation type.
     */
    public void setRequiredOf(boolean requiredOf) {
        this.isRequiredOf = requiredOf;
    }

    /**
     * Gets the "from" type, or domain of this relation type
     */
    public Type getFrom() {
        return getField(link -> link.from);
    }

    /**
     * Sets the "from" type, or domain of this relation type
     */
    public void setFrom(Type from) {
        this.from = from;
    }

    /**
     * Gets the "to" type, or range of this relation type
     */
    public Type getTo() {
        return getField(link -> link.to);
    }

    /**
     * Sets the "to" type, or range of this relation type
     */
    public void setTo(Type to) {
        this.to = to;
    }

    /**
     * Gets the cardinality restriction of this relation type
     */
    public Cardinality getCardinality() {
        return getField(link -> link.cardinality);
    }

    /**
     * Sets the cardinality restriction of this relation type
     */
    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    /**
     * Gets the unidirectionality of this relation type. A relation type is unidirectional if it is only to be
     * traversed from domain to range, never from range to domain.
     */
    public boolean getUnidirected() {
        return getField(link -> link.unidirected);
    }

    /**
     * Sets the unidirectionality of this relation type.
     */
    public void setUnidirected(boolean unidirected) {
        this.unidirected = unidirected;
    }

    @Override
    public String toString() {
        return "RelationType[" + getLabel() + "]";
    }
}
