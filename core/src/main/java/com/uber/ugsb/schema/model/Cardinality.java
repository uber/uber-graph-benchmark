package com.uber.ugsb.schema.model;

/**
 * The cardinality of a link (relationship)
 */
public enum Cardinality {

    ManyToMany(Part.Many, Part.Many), // the default for edges
    ManyToOne(Part.Many, Part.One), // aka "functional"; the default for properties
    OneToMany(Part.One, Part.Many), // aka "inverse functional"
    OneToOne(Part.One, Part.One);

    /**
     * One half of a cardinality restriction. E.g. in "ManyToOne", the range part is the restriction that,
     * for any given object in the domain, there is at most one object in the domain.
     */
    public enum Part {
        One, Many
    }

    private final Part domainPart;
    private final Part rangePart;

    Cardinality(final Part domainPart, final Part rangePart) {
        this.domainPart = domainPart;
        this.rangePart = rangePart;
    }

    /**
     * Gets the domain part of the cardinality, e.g. "Many" in "ManyToOne"
     */
    public Part getDomainPart() {
        return domainPart;
    }

    /**
     * Gets the range part of the cardinality, e.g. "One" in "ManyToOne"
     */
    public Part getRangePart() {
        return rangePart;
    }
}
