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

package com.uber.ugb.schema;

import com.google.common.base.Preconditions;
import com.uber.ugb.schema.model.Cardinality;
import com.uber.ugb.schema.model.EntityType;
import com.uber.ugb.schema.model.Index;
import com.uber.ugb.schema.model.RelationType;
import com.uber.ugb.schema.model.Schema;
import com.uber.ugb.schema.model.SchemaElement;
import com.uber.ugb.schema.model.Type;
import com.uber.ugb.schema.model.dto.EntityTypeDTO;
import com.uber.ugb.schema.model.dto.IndexDTO;
import com.uber.ugb.schema.model.dto.InlineRelationTypeDTO;
import com.uber.ugb.schema.model.dto.RelationTypeDTO;
import com.uber.ugb.schema.model.dto.SchemaDTO;
import com.uber.ugb.schema.model.dto.TypeDTO;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * A utility which consumes a set of schema DTOs and produces a corresponding vocabulary of schemas.
 * This vocabulary is validated according to all of the constraints of the schema specification;
 * this class guarantees that only a valid vocabulary is produced.
 * A valid vocabulary may be rejected by a storage-specific mapper if it uses features supported at the schema
 * level but not at the storage level (such as links to or from other links).
 */
public final class SchemaBuilder {
    private static final String SCHEMA_NAME_REGEX = "[a-z][a-z0-9_]*";
    private static final String ENTITY_TYPE_NAME_REGEX = "[A-Z][a-z0-9]*([A-Z][a-z0-9]*)*";
    private static final String RELATION_TYPE_NAME_REGEX = "[a-z][a-z0-9]*([A-Z][a-z0-9]*)*";
    private static final Pattern SCHEMA_NAME_PATTERN = Pattern.compile(SCHEMA_NAME_REGEX);
    private static final Pattern ENTITY_TYPE_NAME_PATTERN = Pattern.compile(ENTITY_TYPE_NAME_REGEX);
    private static final Pattern RELATION_TYPE_NAME_PATTERN = Pattern.compile(RELATION_TYPE_NAME_REGEX);

    private final Map<String, SchemaDTO> schemaDTOs = new LinkedHashMap<>();
    private Map<String, Schema> schemasByName;
    private Stack<SchemaElement> contextStack;

    private boolean requireDescriptions;

    /**
     * Specifies whether descriptions are required for all schema elements (schemas and types).
     * If this flag is set and any descriptions are missing, schema validation will fail with an
     * appropriate error message.
     */
    public void requireDescriptions(boolean requireDescriptions) {
        this.requireDescriptions = requireDescriptions;
    }

    /**
     * Creates a valid, self consistent vocabulary based on the schema DTOs added thus far.
     *
     * @return a valid vocabulary
     * @throws InvalidSchemaException if the provided schema DTOs are not valid, complete, or self-consistent.
     */
    public synchronized Vocabulary toVocabulary() throws InvalidSchemaException {
        schemasByName = new HashMap<>();

        try {
            materializeAndValidateVocabulary();
        } catch (InvalidSchemaException e) {
            throw new InvalidSchemaException("in " + contextStackToString() + ": " + e.getMessage(), e);
        }

        return new Vocabulary(schemasByName.values());
    }

    /**
     * Adds zero or more schemas to this builder
     */
    public void addSchemas(final SchemaDTO... schemaDTOs) {
        for (SchemaDTO schemaDTO : schemaDTOs) {
            addSchema(schemaDTO);
        }
    }

    /**
     * Adds zero or more schemas to this builder
     */
    public void addSchemas(final Collection<SchemaDTO> schemaDTOs) {
        for (SchemaDTO schemaDTO : schemaDTOs) {
            addSchema(schemaDTO);
        }
    }

    /**
     * Adds to this builder a schema from a YAML input stream
     */
    public SchemaDTO addSchema(final InputStream yamlStream) throws InvalidSchemaException {
        SchemaDTO schema;
        try {
            schema = new Yaml().loadAs(yamlStream, SchemaDTO.class);
        } catch (Exception e) {
            throw new InvalidSchemaException("schema YAML parse error", e);
        }
        addSchema(schema);
        return schema;
    }

    /**
     * Adds all schemas in a given directory, recursively, to this builder.
     * All files with the extension ".yaml" will be parsed as schemas.
     */
    public void addSchemasRecursively(final File yamlDir)
            throws IOException, InvalidSchemaException {
        addSchemasRecursively(yamlDir, ".yaml");
    }

    /**
     * Adds all schemas in a given directory, recursively, to this builder.
     * All files with the suffix will be parsed as schemas.
     */
    public void addSchemasRecursively(final File yamlDir, String suffix)
        throws IOException, InvalidSchemaException {
        Preconditions.checkNotNull(yamlDir);
        if (!yamlDir.exists()) {
            throw new InvalidSchemaException("directory does not exist: " + yamlDir);
        }
        if (!yamlDir.isDirectory()) {
            throw new InvalidSchemaException("not a directory: " + yamlDir);
        }
        for (File file : yamlDir.listFiles()) {
            if (file.getName().endsWith(suffix)) {
                addSchema(file);
            } else if (file.isDirectory()) {
                addSchemasRecursively(file);
            }
        }
    }

    /**
     * Adds to this builder a schema defined in the given YAML file
     */
    public SchemaDTO addSchema(final File yamlFile) throws IOException, InvalidSchemaException {
        if (yamlFile.exists() && !yamlFile.isDirectory() && yamlFile.canRead()) {
            try (InputStream input = new FileInputStream(yamlFile)) {
                return addSchema(input);
            } catch (InvalidSchemaException e) {
                throw new InvalidSchemaException(
                        "when reading file " + yamlFile.getAbsolutePath() + ": " + e.getMessage(), e.getCause());
            }
        } else {
            throw new IllegalArgumentException("can't read input file " + yamlFile.getAbsolutePath());
        }
    }

    /**
     * Gets all schema DTOs (uninterpreted schemas) already added to this builder
     */
    public Map<String, SchemaDTO> getSchemaDTOs() {
        return schemaDTOs;
    }

    public void addSchema(final SchemaDTO schemaDTO) {
        Preconditions.checkNotNull(schemaDTO.getName());
        schemaDTOs.put(schemaDTO.getName(), schemaDTO);
    }

    private static <T> void checkUnique(Collection<T> elements, Function<T, String> nameGetter)
            throws InvalidSchemaException {
        Set<String> names = new HashSet<>();
        for (T element : elements) {
            String name = nameGetter.apply(element);
            if (names.contains(name)) {
                throw new InvalidSchemaException("duplicate element '" + name + "'");
            } else {
                names.add(name);
            }
        }
    }

    private static <T> void checkUnique(T[] elements, Function<T, String> nameGetter) throws InvalidSchemaException {
        checkUnique(Arrays.asList(elements), nameGetter);
    }

    private void materializeAndValidateVocabulary() throws InvalidSchemaException {
        contextStack = new Stack<>();

        materializeSchemas();
        materializeImports();
        expandInlineLinks();
        materializeEntities();
        materializeIndexes();
        materializeExtensions();
        materializeDomainsAndRanges();

        checkNamingConventionsRespected();
        checkExtensionsAreNonCyclical();
        checkImportsAreNoncyclical();

        if (requireDescriptions) {
            checkDescriptions();
        }
    }

    private void materializeSchemas() throws InvalidSchemaException {
        checkUnique(schemaDTOs.values(), SchemaDTO::getName);
        for (SchemaDTO schemaDTO : schemaDTOs.values()) {
            contextStack.push(schemaDTO);

            checkRequiredAndNonempty(schemaDTO.getName(), "name");
            Schema schema = new Schema(schemaDTO.getName());
            schemasByName.put(schema.getName(), schema);

            contextStack.pop();
        }
    }

    private void materializeImports() throws InvalidSchemaException {
        for (SchemaDTO schemaDTO : schemaDTOs.values()) {
            Schema schema = schemasByName.get(schemaDTO.getName());
            contextStack.push(schema);

            List<Schema> imports = new LinkedList<>();
            for (String ref : schemaDTO.getIncludes()) {
                checkRequiredAndNonempty(ref, "name");

                Schema child = schemasByName.get(ref);
                if (null == child) {
                    throw new InvalidSchemaException("no such schema: " + ref);
                }

                contextStack.push(child);
                imports.add(child);
                contextStack.pop();
            }
            checkUnique(schemaDTO.getIncludes(), s -> s);
            schema.setIncludes(imports);

            contextStack.pop();
        }
    }

    private void checkTypeNamesUnique(final SchemaDTO schemaDTO) throws InvalidSchemaException {
        Set<TypeDTO> allTypes = new HashSet<>();
        allTypes.addAll(Arrays.asList(schemaDTO.getEntities()));
        checkUnique(allTypes, TypeDTO::getLabel);
        allTypes.clear();
        allTypes.addAll(Arrays.asList(schemaDTO.getRelations()));
        checkUnique(allTypes, TypeDTO::getLabel);
    }

    private void materializeEntities() throws InvalidSchemaException {
        for (SchemaDTO schemaDTO : schemaDTOs.values()) {
            Schema schema = schemasByName.get(schemaDTO.getName());
            copySchemaElementFields(schemaDTO, schema);
            contextStack.push(schema);

            materializeTypes(schemaDTO, schema);
            materializeLinks(schemaDTO, schema);

            checkTypeNamesUnique(schemaDTO);

            contextStack.pop();
        }
    }

    private boolean isCoreSchema(final SchemaDTO schemaDTO) {
        return schemaDTO.getName().equals(Vocabulary.CORE_SCHEMA_NAME);
    }

    private boolean isCoreSchema(final Schema schema) {
        return schema.getName().equals(Vocabulary.CORE_SCHEMA_NAME);
    }

    private void materializeTypes(final SchemaDTO schemaDTO, final Schema schema) throws InvalidSchemaException {
        checkUnique(schemaDTO.getEntities(), TypeDTO::getLabel);
        Map<String, EntityType> types = new HashMap<>();
        for (EntityTypeDTO entityTypeDTO : schemaDTO.getEntities()) {
            if (!isCoreSchema(schemaDTO) && (null == entityTypeDTO.getExtends()
                    || 0 == entityTypeDTO.getExtends().length)) {
                entityTypeDTO.setExtends(new String[]{Vocabulary.THING_NAME.toString()});
            }

            EntityType entityType = new EntityType(schema, entityTypeDTO.getLabel());
            contextStack.push(entityType);

            copyFields(entityTypeDTO, entityType);
            checkEquivalentTo(entityType);
            entityType.setValues(entityTypeDTO.getValues());
            types.put(entityType.getLabel(), entityType);

            contextStack.pop();
        }
        schema.setEntityTypes(types);
    }

    private void materializeLinks(final SchemaDTO schemaDTO, final Schema schema) throws InvalidSchemaException {
        checkUnique(schemaDTO.getRelations(), TypeDTO::getLabel);
        Map<String, RelationType> links = new HashMap<>();
        for (RelationTypeDTO typeDTO : schemaDTO.getRelations()) {
            RelationType relationType = new RelationType(schema, typeDTO.getLabel());
            contextStack.push(relationType);

            copyLinkFields(typeDTO, relationType);
            checkEquivalentTo(relationType);
            links.put(relationType.getLabel(), relationType);

            contextStack.pop();
        }
        schema.setRelationTypes(links);
    }

    private void materializeIndexes() throws InvalidSchemaException {
        for (SchemaDTO schemaDTO : schemaDTOs.values()) {
            Schema schema = schemasByName.get(schemaDTO.getName());
            contextStack.push(schema);

            List<Index> indexes = new LinkedList<>();
            for (IndexDTO indexDTO : schemaDTO.getIndexes()) {
                String linkName = indexDTO.getKey();
                if (null == linkName) {
                    throw new InvalidSchemaException("missing key for index");
                }
                RelationType relationType = getRelationType(linkName, schema, true);
                checkNotAbstract(relationType);
                Index index = new Index(relationType);

                if (null != indexDTO.getOrderBy()) {
                    RelationType orderBy = getRelationType(indexDTO.getOrderBy(), schema, true);
                    checkNotAbstract(orderBy);
                    index.setOrderBy(orderBy);
                    index.setOrder(indexDTO.getOrder());
                    index.setDirection(indexDTO.getDirection());
                }

                indexes.add(index);
            }
            schema.setIndexes(indexes);

            contextStack.pop();
        }
    }

    private void materializeExtensions() throws InvalidSchemaException {
        for (SchemaDTO schemaDTO : schemaDTOs.values()) {
            Schema schema = schemasByName.get(schemaDTO.getName());
            contextStack.push(schema);

            for (EntityTypeDTO entityTypeDTO : schemaDTO.getEntities()) {
                materializeExtensions(schema, entityTypeDTO);
            }

            for (RelationTypeDTO typeDTO : schemaDTO.getRelations()) {
                materializeExtensions(schema, typeDTO);
            }

            contextStack.pop();
        }

        for (Schema schema : schemasByName.values()) {
            contextStack.push(schema);
            for (EntityType entityType : schema.getEntityTypes().values()) {
                contextStack.push(entityType);
                checkExtendsBasicDataType(entityType, schema);
                checkValues(entityType);
                contextStack.pop();
            }
            contextStack.pop();
        }
    }

    private void materializeExtensions(final Schema schema, final EntityTypeDTO entityTypeDTO)
            throws InvalidSchemaException {
        EntityType entityType = getEntityType(entityTypeDTO.getLabel(), schema, true);
        contextStack.push(entityType);

        List<EntityType> extended = new LinkedList<>();
        for (String ext : entityTypeDTO.getExtends()) {
            extended.add(getEntityType(ext, schema, true));
        }
        entityType.setExtends(extended);

        contextStack.pop();
    }

    private void materializeExtensions(final Schema schema, final RelationTypeDTO typeDTO)
            throws InvalidSchemaException {
        RelationType type = getRelationType(typeDTO.getLabel(), schema, true);
        contextStack.push(type);

        List<RelationType> extended = new LinkedList<>();
        for (String ext : typeDTO.getExtends()) {
            extended.add(getRelationType(ext, schema, true));
        }
        type.setExtends(extended);

        contextStack.pop();
    }

    private void materializeDomainsAndRanges() throws InvalidSchemaException {
        for (SchemaDTO schemaDTO : schemaDTOs.values()) {
            Schema schema = schemasByName.get(schemaDTO.getName());
            contextStack.push(schema);

            for (RelationTypeDTO typeDTO : schemaDTO.getRelations()) {
                RelationType relationType = getRelationType(typeDTO.getLabel(), schema, true);
                contextStack.push(relationType);

                Type mapsFrom = getType(typeDTO.getFrom(), schema, true);
                Type mapsTo = getType(typeDTO.getTo(), schema, true);
                relationType.setFrom(mapsFrom);
                relationType.setTo(mapsTo);
                correctLinkCardinality(relationType);

                if (!relationType.getAbstract()) {
                    if (null == relationType.getFrom()) {
                        throw new InvalidSchemaException("missing 'from'");
                    } else if (null == relationType.getTo()) {
                        throw new InvalidSchemaException("missing 'to'");
                    }
                }

                contextStack.pop();
            }

            contextStack.pop();
        }

        // check domain and range only after all links have been constructed
        for (Schema schema : schemasByName.values()) {
            contextStack.push(schema);

            for (RelationType relationType : schema.getRelationTypes().values()) {
                contextStack.push(relationType);

                checkMapsFrom(relationType);
                checkMapsTo(relationType);

                contextStack.pop();
            }

            contextStack.pop();
        }
    }

    private void expandInlineLinks() throws InvalidSchemaException {
        for (SchemaDTO schemaDTO : schemaDTOs.values()) {
            contextStack.push(schemaDTO);

            List<RelationTypeDTO> linkTypeDTOS = new ArrayList<>();
            linkTypeDTOS.addAll(Arrays.asList(schemaDTO.getRelations()));
            for (EntityTypeDTO entityTypeDTO : schemaDTO.getEntities()) {
                contextStack.push(entityTypeDTO);
                expandInlineLinks(entityTypeDTO, linkTypeDTOS);
                contextStack.pop();
            }
            for (RelationTypeDTO typeDTO : schemaDTO.getRelations()) {
                contextStack.push(typeDTO);
                expandInlineLinks(typeDTO, linkTypeDTOS);
                contextStack.pop();
            }
            schemaDTO.setRelations(linkTypeDTOS.toArray(new RelationTypeDTO[linkTypeDTOS.size()]));

            contextStack.pop();
        }
    }

    private void expandInlineLinks(final TypeDTO typeDTO, List<RelationTypeDTO> linkTypeDTOS)
            throws InvalidSchemaException {
        for (InlineRelationTypeDTO inlineDTO : typeDTO.getRelations()) {
            contextStack.push(inlineDTO);

            RelationTypeDTO link = expand(inlineDTO, typeDTO);
            linkTypeDTOS.add(link);
            // expand recursively
            expandInlineLinks(link, linkTypeDTOS);

            contextStack.pop();
        }
        typeDTO.setRelations(new InlineRelationTypeDTO[]{});
    }

    private RelationTypeDTO expand(final InlineRelationTypeDTO from, TypeDTO domain) throws InvalidSchemaException {
        RelationTypeDTO to = new RelationTypeDTO();
        copyFields(from, to);

        to.setCardinality(from.getCardinality());
        to.setRequired(from.getRequired());
        to.setRequiredOf(from.getRequiredOf());
        to.setTo(from.getTo());
        to.setRelations(from.getRelations());
        to.setFrom(domain.getLabel());

        return to;
    }

    private void checkMapsFrom(final RelationType relationType) throws InvalidSchemaException {
        Type domain = relationType.getFrom();
        if (!relationType.getAbstract()) {
            checkRequiredField(domain, "mapsFrom");
            if (domain instanceof EntityType && ((EntityType) domain).getIsDataType()) {
                throw new InvalidSchemaException("a relationType cannot map from a datatype");
            }
        }
    }

    private void checkMapsTo(final RelationType relationType) throws InvalidSchemaException {
        Type type = relationType.getTo();
        if (!relationType.getAbstract()) {
            checkRequiredField(type, "mapsTo");

            if (type instanceof RelationType) {
                throw new InvalidSchemaException("a relationType cannot map to another relationType");
            }
        }
    }

    private void checkValues(final EntityType entityType) throws InvalidSchemaException {
        Object[] values = entityType.getValues();

        if (null != values) {
            checkUnique(values, Object::toString);

            String base = entityType.getSimpleBaseType().getLabel();
            for (Object value : values) {
                Class objectClass = value.getClass();
                if (!objectClass.getSimpleName().equals(base)) {
                    throw new InvalidSchemaException("value " + value + " is not valid for this entity type; "
                            + "expected values of class " + base);
                }
            }
        }
    }

    private void checkEquivalentTo(final Type type) throws InvalidSchemaException {
        if (null != type.getSameAs()) {
            try {
                new URI(type.getSameAs());
            } catch (URISyntaxException e) {
                throw new InvalidSchemaException("equivalentTo field is not a valid URI: "
                        + type.getSameAs());
            }
        }
    }

    private void checkExtendsBasicDataType(final EntityType entityType, final Schema schema)
            throws InvalidSchemaException {
        if (!isCoreSchema(schema)) {
            checkArgument(null != entityType.getExtends() && 0 < entityType.getExtends().size(),
                    "non-core " + entityType + " must extend a basic entity type");
        }

        // side-effect: sets isDataType
        entityType.getSimpleBaseType();
    }

    private void checkImportsAreNoncyclical() throws InvalidSchemaException {
        for (Schema schema : schemasByName.values()) {
            if (SchemaUtils.hasCycle(schema, Schema::getIncludes)) {
                throw new InvalidSchemaException("imported schemas contain a loop: " + schema);
            }
        }
    }

    private void checkNotAbstract(final RelationType relationType) throws InvalidSchemaException {
        if (relationType.getAbstract()) {
            throw new InvalidSchemaException("materialized relationType " + relationType + " is abstract");
        }
    }

    private void checkDescriptions() {
        for (Schema schema : schemasByName.values()) {
            contextStack.push(schema);
            checkSchemaDescription(schema);
            checkEntityTypeDescriptions(schema);
            checkRelationTypeDescriptions(schema);
            contextStack.pop();
        }
    }

    private void checkSchemaDescription(final Schema schema) {
        checkDescription(schema);
    }

    private void checkEntityTypeDescriptions(final Schema schema) {
        for (EntityType type : schema.getEntityTypes().values()) {
            contextStack.push(type);
            checkDescription(type);
            contextStack.pop();
        }
    }

    private void checkRelationTypeDescriptions(final Schema schema) {
        for (RelationType type : schema.getRelationTypes().values()) {
            contextStack.push(type);
            checkDescription(type);
            contextStack.pop();
        }
    }

    private void checkDescription(final SchemaElement element) {
        if (null == element.getDescription() || 0 == element.getDescription().length()) {
            throw new InvalidSchemaException("missing description");
        }
    }

    private <X extends Exception> void forAllSchemas(final ConsumerWithException<Schema, X> consumer) throws X {
        for (Schema schema : schemasByName.values()) {
            consumer.accept(schema);
        }
    }

    private <X extends Exception> void forAllEntityTypes(final ConsumerWithException<EntityType, X> consumer) throws X {
        forAllSchemas(schema -> {
            for (EntityType entityType : schema.getEntityTypes().values()) {
                consumer.accept(entityType);
            }
        });
    }

    private <X extends Exception> void forAllRelationTypes(final ConsumerWithException<RelationType, X> consumer)
            throws X {
        forAllSchemas(schema -> {
            for (RelationType relationType : schema.getRelationTypes().values()) {
                consumer.accept(relationType);
            }
        });
    }

    private void checkNamingConventionsRespected() throws InvalidSchemaException {
        forAllSchemas((ConsumerWithException<Schema, InvalidSchemaException>) schema -> {
            contextStack.push(schema);
            checkSchemaNameIsValid(schema);
            forAllEntityTypes((ConsumerWithException<EntityType, InvalidSchemaException>) entityType -> {
                contextStack.push(entityType);
                checkEntityTypeNameIsValid(entityType);
                contextStack.pop();
            });
            forAllRelationTypes((ConsumerWithException<RelationType, InvalidSchemaException>) relationType -> {
                contextStack.push(relationType);
                checkRelationTypeNameIsValid(relationType);
                contextStack.pop();
            });
            contextStack.pop();
        });
    }

    private void checkSchemaNameIsValid(final Schema schema) {
        checkName(schema.getName(), SCHEMA_NAME_PATTERN, "schema name", SCHEMA_NAME_REGEX);
    }

    private void checkEntityTypeNameIsValid(final EntityType type) {
        checkName(type.getLabel(), ENTITY_TYPE_NAME_PATTERN, "entity type label", ENTITY_TYPE_NAME_REGEX);
    }

    private void checkRelationTypeNameIsValid(final RelationType type) {
        checkName(type.getLabel(), RELATION_TYPE_NAME_PATTERN, "relation type label", RELATION_TYPE_NAME_REGEX);
    }

    private void checkName(final String name,
                           final Pattern pattern,
                           final String kind,
                           final String regex) {
        if (!pattern.matcher(name).matches()) {
            throw new InvalidSchemaException(kind + " does not match regex: " + regex);
        }
    }

    private void checkExtensionsAreNonCyclical() throws InvalidSchemaException {
        forAllEntityTypes(this::checkExtensionsAreNonCyclical);
        forAllRelationTypes(this::checkExtensionsAreNonCyclical);
    }

    private void checkExtensionsAreNonCyclical(final EntityType entityType) throws InvalidSchemaException {
        if (SchemaUtils.hasCycle(entityType, EntityType::getExtends)) {
            throw new InvalidSchemaException("extended types contain a loop: " + entityType);
        }
    }

    private void checkExtensionsAreNonCyclical(final RelationType relationType) throws InvalidSchemaException {
        if (SchemaUtils.hasCycle(relationType, RelationType::getExtends)) {
            throw new InvalidSchemaException("extended links contain a loop: " + relationType);
        }
    }

    private void copySchemaElementFields(final SchemaElement from, final SchemaElement to) {
        to.setDescription(from.getDescription());
        to.setComment(from.getComment());
    }

    private void copyLinkFields(final RelationTypeDTO from, final RelationType to)
            throws InvalidSchemaException {
        copyFields(from, to);
        to.setCardinality(from.getCardinality());
        to.setRequired(from.getRequired());
        to.setRequiredOf(from.getRequiredOf());
        to.setUnidirected(from.getUnidirected());
    }

    private void copyFields(final TypeDTO from, final Type to)
            throws InvalidSchemaException {
        copySchemaElementFields(from, to);
        to.setAbstract(from.getAbstract());
        to.setSameAs(from.getSameAs());
        checkRequiredAndNonempty(to.getLabel(), "label");
    }

    private void copyFields(final TypeDTO from, final TypeDTO to)
            throws InvalidSchemaException {
        copySchemaElementFields(from, to);
        to.setAbstract(from.getAbstract());
        to.setSameAs(from.getSameAs());
        to.setExtends(from.getExtends());
        to.setLabel(from.getLabel());
        checkRequiredAndNonempty(to.getLabel(), "label");
    }

    private void correctLinkCardinality(final RelationType relationType) {
        if (!relationType.getAbstract() && null == getInferred(relationType, RelationType::getCardinality)) {
            EntityType range = (EntityType) getInferred(relationType, RelationType::getTo);
            if (null != range) {
                if (range.getIsDataType()) {
                    relationType.setCardinality(Cardinality.ManyToOne);
                } else {
                    relationType.setCardinality(Cardinality.ManyToMany);
                }
            }
        }
    }

    private <T> T getInferred(final RelationType relationType, final Function<RelationType, T> accessor) {
        Stack<RelationType> stack = new Stack<>();
        stack.push(relationType);
        while (!stack.isEmpty()) {
            RelationType cur = stack.pop();
            T value = accessor.apply(cur);
            if (null != value) {
                return value;
            }
            for (RelationType parent : cur.getExtends()) {
                stack.push(parent);
            }
        }
        return null;
    }

    private Type getType(final String name, final Schema refSchema, boolean required)
            throws InvalidSchemaException {
        if (null == name) {
            return null;
        }

        Type type = getEntityType(name, refSchema, false);
        if (null == type) {
            type = getRelationType(name, refSchema, required);
        }
        return type;
    }

    private EntityType getEntityType(final String name, final Schema refSchema, final boolean required)
            throws InvalidSchemaException {
        return getType(name, refSchema, required, Schema::getEntityTypes, "entity");
    }

    private RelationType getRelationType(final String name, final Schema refSchema, final boolean required)
            throws InvalidSchemaException {
        return getType(name, refSchema, required, Schema::getRelationTypes, "relation");
    }

    private <T extends Type> T getType(final String name,
                                       final Schema refSchema,
                                       final boolean required,
                                       final Function<Schema, Map<String, T>> toTypes,
                                       final String kind) {
        QualifiedName qn;
        // first try the name as a local name (regardless of whether it contains a '.')
        qn = new QualifiedName(null, name);
        T type = getType(qn, refSchema, toTypes);
        if (null == type) {
            // if the above was unsuccessful, try to parse the name as a qualified name
            qn = new QualifiedName(name);
            type = getType(qn, refSchema, toTypes);
            if (required && null == type) {
                noSuchElement(qn, kind + " type");
            }
        }
        return type;
    }

    private <T extends Type> T getType(final QualifiedName qn,
                                       final Schema refSchema,
                                       final Function<Schema, Map<String, T>> toTypes) {
        Schema schema = getSchemaForName(qn, refSchema);
        return toTypes.apply(schema).get(qn.getLocalName());
    }

    private void noSuchElement(final QualifiedName qn, final String label) throws InvalidSchemaException {
        String message = "no such " + label + ": " + qn;
        QualifiedName suggestion = suggestQualifiedName(qn.getLocalName());
        if (null != suggestion) {
            message = message + ". Did you mean " + suggestion + "?";
        }
        throw new InvalidSchemaException(message);
    }

    private QualifiedName suggestQualifiedName(final String name) throws InvalidSchemaException {
        for (Schema schema : schemasByName.values()) {
            Type type = getType(name, schema, false);
            if (null != type) {
                return new QualifiedName(schema.getName(), type.getLabel());
            }
        }

        return null;
    }

    private Schema getSchemaForName(final QualifiedName name, final Schema refSchema) throws InvalidSchemaException {
        if (null == name.getPrefix()) {
            return refSchema;
        } else {
            Schema schema = schemasByName.get(name.getPrefix());
            if (null == schema) {
                throw new InvalidSchemaException("no such schema: " + name.getPrefix());
            }
            return schema;
        }
    }

    protected <T> void checkRequiredField(final T field, final String fieldName) throws InvalidSchemaException {
        if (null == field) {
            throw new InvalidSchemaException(fieldName + " is required");
        }
    }

    protected void checkRequiredAndNonempty(final String field, final String fieldName) throws InvalidSchemaException {
        checkRequiredField(field, fieldName);
        checkArgument(0 < field.trim().length(), fieldName + " may not be empty");
    }

    protected void checkArgument(final boolean argument, final String failureMessage) throws InvalidSchemaException {
        if (!argument) {
            throw new InvalidSchemaException(failureMessage);
        }
    }

    private String contextStackToString() {
        StringBuilder result = new StringBuilder();
        while (!contextStack.isEmpty()) {
            SchemaElement el = contextStack.pop();
            if (result.length() > 0) {
                result.insert(0, " > ");
            }
            result.insert(0, el.toString());
        }
        return result.toString();
    }

    private interface ConsumerWithException<T, X extends Exception> {
        void accept(T t) throws X;
    }
}
