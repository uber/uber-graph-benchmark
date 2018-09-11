package com.uber.ugb.schema;

import com.uber.ugb.schema.model.EntityType;
import com.uber.ugb.schema.model.Index;
import com.uber.ugb.schema.model.RelationType;
import com.uber.ugb.schema.model.Schema;
import com.uber.ugb.schema.model.Type;
import com.uber.ugb.schema.model.dto.EntityTypeDTO;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A collection of graph schemas which are used together.
 * Each schema in a vocabulary must be valid, and the schemas must be complete and consistent with one another.
 */
public class Vocabulary implements Serializable {

    /**
     * The name of the only required schema; every vocabulary must have a schema named "core", in which
     * the entity type "Thing" and the basic data types are assumed to be defined.
     */
    public static final String CORE_SCHEMA_NAME = "core";

    /**
     * A reference to the built-in Thing type, which is the base type for all structured entity types
     */
    public static final QualifiedName THING_NAME = new QualifiedName(CORE_SCHEMA_NAME, "Thing");

    /**
     * Version ID for the schema YAML format, and by extension, other uGraph serial formats
     */
    public static final long serialVersionUID = 0;

    /**
     * An enumeration of basic data types
     */
    public enum BasicType {
        Boolean("Boolean", Boolean.class),
        Decimal("Decimal", Double.class),
        Double("Double", Double.class),
        Float("Float", Float.class),
        Integer("Integer", Integer.class),
        Long("Long", Long.class),
        String("String", String.class);

        private final String name;
        private final Class javaClass;

        BasicType(String name, Class javaClass) {
            this.name = name;
            this.javaClass = javaClass;
        }

        public java.lang.String getName() {
            return name;
        }

        public QualifiedName getQName() {
            return new QualifiedName(CORE_SCHEMA_NAME, getName());
        }

        public Class getJavaEquivalent() {
            return javaClass;
        }

        public EntityTypeDTO getEntityTypeDTO() {
            return new EntityTypeDTO(new QualifiedName(CORE_SCHEMA_NAME, getName()).toString());
        }
    }

    private final List<Schema> schemas = new LinkedList<>();
    private final Map<String, Schema> schemasByName = new HashMap<>();
    private final Map<QualifiedName, EntityType> entityTypesByQualifiedName;
    private final Map<QualifiedName, RelationType> relationTypesByQualifiedName;
    private final Map<String, Set<EntityType>> entityTypesByLocalName;
    private final Map<String, Set<RelationType>> relationTypesByLocalName;

    Vocabulary(final Collection<Schema> schemasToAdd) throws InvalidSchemaException {
        schemas.addAll(schemasToAdd);
        for (Schema schema : schemasToAdd) {
            schemasByName.put(schema.getName(), schema);
        }

        entityTypesByQualifiedName = createEntityTypesByQualifiedName();
        relationTypesByQualifiedName = createRelationTypesByQualifiedName();

        entityTypesByLocalName = createEntityTypesByLocalName();
        relationTypesByLocalName = createRelationTypesByLocalName();
    }

    /**
     * Gets all schemas in the vocabulary
     */
    public List<Schema> getSchemas() {
        return schemas;
    }

    /**
     * Gets all entity types in the vocabulary
     */
    public Map<QualifiedName, EntityType> getEntityTypes() {
        return entityTypesByQualifiedName;
    }

    /**
     * Gets all relation types in the vocabulary
     */
    public Map<QualifiedName, RelationType> getRelationTypes() {
        return relationTypesByQualifiedName;
    }

    /**
     * Gets all relation types which map from the given type
     * @param domainType the domain ("from" type) of the desired relation types
     */
    public <T extends Type> Set<RelationType> getConcreteRelationTypesFrom(final T domainType) {
        Set<T> inferredTypes = domainType.getInferredTypes();
        Set<RelationType> typesFrom = new HashSet<>();
        for (RelationType relationType : getRelationTypes().values()) {
            if (relationType.getAbstract()) {
                continue;
            }
            Type from = relationType.getFromInferred();
            if (inferredTypes.contains(from)) {
                typesFrom.add(relationType);
            }
        }
        return typesFrom;
    }

    /**
     * Gets all index hints in the vocabulary
     */
    public Set<Index> getIndexes() {
        Set<Index> all = new HashSet<>();
        for (Schema schema : schemas) {
            if (null != schema.getIndexes()) {
                all.addAll(schema.getIndexes());
            }
        }
        return all;
    }

    /**
     * Gets a schema by name
     */
    public Optional<Schema> getSchema(final String name) {
        Schema schema = schemasByName.get(name);
        return null == schema ? Optional.empty() : Optional.of(schema);
    }

    /**
     * Gets an entity type by local name
     */
    public EntityType getEntityType(final String localName) {
        Set<EntityType> types = entityTypesByLocalName.get(localName);
        if (null == types || 0 == types.size()) {
            throw new IllegalArgumentException("no entity type with label " + localName);
        } else if (types.size() > 1) {
            throw new IllegalArgumentException("entity type label is ambiguous: " + localName);
        }
        return types.iterator().next();
    }

    /**
     * Gets an entity type by qualified name
     */
    public EntityType getEntityType(final QualifiedName qName) {
        EntityType el = entityTypesByQualifiedName.get(qName);
        if (null == el) {
            throw new IllegalArgumentException("no such entity type: " + qName);
        }
        return el;
    }

    /**
     * Gets a relation type by local name.
     * Fails if this label is ambiguous within the given domain category (entity or relation).
     */
    public RelationType getRelationType(final String localName, final SchemaManager.TypeCategory fromCategory) {
        Set<RelationType> types = relationTypesByLocalName.get(localName);
        if (null == types || 0 == types.size()) {
            throw new IllegalArgumentException("no relation type with label " + localName);
        } else if (types.size() > 1) {
            RelationType found = null;
            for (RelationType type : types) {
                if ((type.getFrom() instanceof RelationType
                        && fromCategory == SchemaManager.TypeCategory.Relation)
                        || (type.getFrom() instanceof EntityType
                        && fromCategory == SchemaManager.TypeCategory.Entity)) {
                    if (null != found) {
                        throw new IllegalArgumentException(
                                fromCategory + " relation type label is ambiguous: " + localName);
                    }
                    found = type;
                }
            }
            if (null == found) {
                throw new IllegalArgumentException("no " + fromCategory + " relation type with label " + localName);
            } else {
                return found;
            }
        }
        return types.iterator().next();
    }

    /**
     * Gets a relation type by qualified name
     */
    public RelationType getRelationType(final QualifiedName name) {
        RelationType el = relationTypesByQualifiedName.get(name);
        if (null == el) {
            throw new IllegalArgumentException("no such relation type: " + name);
        }
        return el;
    }

    /**
     * Finds the datatype for the given relation type
     */
    public static Vocabulary.BasicType dataTypeOf(final RelationType type) {
        return Vocabulary.BasicType.valueOf(((EntityType) type.getTo()).getSimpleBaseType().getLabel());
    }

    private Map<String, Set<EntityType>> createEntityTypesByLocalName() {
        Map<String, Set<EntityType>> all = new HashMap<>();
        for (EntityType entityType : getEntityTypes().values()) {
            Set<EntityType> entityTypes = all.computeIfAbsent(entityType.getLabel(), k -> new HashSet<>());
            entityTypes.add(entityType);
        }
        return all;
    }

    private Map<String, Set<RelationType>> createRelationTypesByLocalName() {
        Map<String, Set<RelationType>> all = new HashMap<>();
        for (RelationType relationType : getRelationTypes().values()) {
            Set<RelationType> relationTypes = all.computeIfAbsent(relationType.getLabel(), k -> new HashSet<>());
            relationTypes.add(relationType);
        }
        return all;
    }

    private Map<QualifiedName, EntityType> createEntityTypesByQualifiedName() {
        Map<QualifiedName, EntityType> all = new HashMap<>();
        for (Schema schema : schemas) {
            if (null != schema.getEntityTypes()) {
                for (EntityType entityType : schema.getEntityTypes().values()) {
                    QualifiedName qName = new QualifiedName(schema.getName(), entityType.getLabel());
                    all.put(qName, entityType);
                }
            }
        }
        return all;
    }

    private Map<QualifiedName, RelationType> createRelationTypesByQualifiedName() {
        Map<QualifiedName, RelationType> all = new HashMap<>();
        for (Schema schema : schemas) {
            if (null != schema.getRelationTypes()) {
                for (RelationType rel : schema.getRelationTypes().values()) {
                    QualifiedName qName = new QualifiedName(schema.getName(), rel.getLabel());
                    all.put(qName, rel);
                }
            }
        }
        return all;
    }
}
