package com.uber.ugb.schema.model.dto;

import com.uber.ugb.schema.model.SchemaElement;

/**
 * A lightweight object representing a schema. These DTOs are used for schema construction and serialization.
 * See <code>Schema</code> for the materialized form of this class.
 */
public class SchemaDTO extends SchemaElement {
    private static final long serialVersionUID = -4196724127419211409L;

    private String name;
    private String[] includes = new String[]{};
    private EntityTypeDTO[] entities = new EntityTypeDTO[]{};
    private RelationTypeDTO[] relations = new RelationTypeDTO[]{};
    private IndexDTO[] indexes = new IndexDTO[]{};

    public SchemaDTO() {
    }

    public SchemaDTO(final String name) {
        this.name = name;
    }

    public String[] getIncludes() {
        return includes;
    }

    public EntityTypeDTO[] getEntities() {
        return entities;
    }

    public RelationTypeDTO[] getRelations() {
        return relations;
    }

    public IndexDTO[] getIndexes() {
        return indexes;
    }

    public void setIncludes(String[] includes) {
        this.includes = includes;
    }

    public void setEntities(EntityTypeDTO[] entities) {
        this.entities = entities;
    }

    public void setRelations(RelationTypeDTO[] relations) {
        this.relations = relations;
    }

    public void setIndexes(IndexDTO[] indexes) {
        this.indexes = indexes;
    }

    public void addImport(final String schemaName) {
        if (null == includes) {
            includes = new String[]{schemaName};
        } else {
            String[] tmp = new String[includes.length + 1];
            System.arraycopy(includes, 0, tmp, 0, includes.length);
            tmp[includes.length] = schemaName;
            includes = tmp;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Schema[" + getName() + "]";
    }
}
