package com.uber.ugb.schema;

import java.io.Serializable;
import java.util.Objects;

/**
 * A type or link name together with a schema name.
 * Qualified names are used to unambiguously refer to an element within a schema.
 * For example, "payments.CreditCard" refers to an element named "CreditCard" within the "payments" schema.
 * Schema names are usually in lowercase, while type names are usually in capitalized camel case and links are in
 * uncapitalized camel case.
 */
public class QualifiedName implements Serializable {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

    private final String prefix;
    private final String localName;

    /**
     * Constructs a qualified name composed of a prefix (schema name) and local name
     */
    public QualifiedName(final String prefix, final String localName) {
        this.prefix = prefix;
        this.localName = localName;
    }

    /**
     * Parses a qualified name into its components.
     * @param qname the qualified name as a dot-separated string. For example, "payments.CreditCard"
     */
    public QualifiedName(final String qname) {
        int i = qname.indexOf('.');
        if (i > -1) {
            prefix = qname.substring(0, i);
            localName = qname.substring(i + 1);
        } else {
            prefix = null;
            localName = qname;
        }
    }

    @Override
    public String toString() {
        return null == getPrefix() ? getLocalName() : getPrefix() + "." + getLocalName();
    }

    /**
     * Gets the prefix (schema name) of this qualified name
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * Gets the local (unqualified) name of this qualified name
     */
    public String getLocalName() {
        return localName;
    }

    @Override
    public boolean equals(final Object that) {
        return that instanceof QualifiedName
                && Objects.equals(this.prefix, ((QualifiedName) that).prefix)
                && Objects.equals(this.localName, ((QualifiedName) that).localName);
    }

    @Override
    public int hashCode() {
        return hashCodeOf(prefix) + 7 * hashCodeOf(localName);
    }

    private int hashCodeOf(final String mayBeNull) {
        return null == mayBeNull ? 0 : mayBeNull.hashCode();
    }
}
