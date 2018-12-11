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

package com.uber.ugb.schema.model.dto;

import com.uber.ugb.schema.model.SchemaElement;

/**
 * A lightweight object representing a type. These DTOs are used for schema construction and serialization.
 * See <code>Type</code> for the materialized form of this abstract class.
 */
public abstract class TypeDTO extends SchemaElement {
    private static final long serialVersionUID = -3996593753845183263L;

    private String label;
    private boolean isAbstract;
    private String sameAs;
    private String[] extended = new String[]{};
    private InlineRelationTypeDTO[] relations = new InlineRelationTypeDTO[]{};

    public TypeDTO() {
    }

    public TypeDTO(final String label) {
        this.label = label;
    }

    public InlineRelationTypeDTO[] getRelations() {
        return relations;
    }

    public void setRelations(InlineRelationTypeDTO[] relations) {
        this.relations = relations;
    }

    public boolean getAbstract() {
        return isAbstract;
    }

    public void setAbstract(boolean anAbstract) {
        isAbstract = anAbstract;
    }

    public String getSameAs() {
        return sameAs;
    }

    public void setSameAs(String sameAs) {
        this.sameAs = sameAs;
    }

    public String[] getExtends() {
        return extended;
    }

    public void setExtends(String[] extended) {
        this.extended = extended;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
        if (label.equals("device_app_source")) {
            System.out.println("break here");
        }
    }
}
