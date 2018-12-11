/*
 *
 *  * Copyright 2018 Uber Technologies Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.uber.ugb.schema.model;

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
