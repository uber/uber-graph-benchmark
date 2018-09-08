package com.uber.ugsb.model;

import com.uber.ugsb.schema.Vocabulary;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

public class PropertyModel implements Serializable {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

    private final Collection<SimpleProperty> properties = new LinkedList<>();

    public <T> void addProperty(final SimpleProperty<T> property) {
        getProperties().add(property);
    }

    public Collection<SimpleProperty> getProperties() {
        return properties;
    }
}
