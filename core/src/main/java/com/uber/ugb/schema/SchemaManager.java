package com.uber.ugb.schema;

import com.uber.ugb.schema.diff.SchemaChange;
import com.uber.ugb.schema.diff.SchemaDiff;
import com.uber.ugb.schema.model.Cardinality;
import com.uber.ugb.schema.model.EntityType;
import com.uber.ugb.schema.model.Index;
import com.uber.ugb.schema.model.RelationType;
import com.uber.ugb.schema.model.Type;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * A utility to mediate between conceptual schemas and the storage schema of a database
 * @param <D> the class of the database or storage schema management API
 */
public abstract class SchemaManager<D> implements Serializable {

    protected static Logger logger = LoggerFactory.getLogger(SchemaManager.class);

    private static final long serialVersionUID = 2830190107857616735L;

    private static final Map<String, Class> valueClassesByName;

    public enum TypeCategory {
        Entity, Relation
    }

    protected enum PGTypeKind {
        Vertex, PropertyValue
    }

    protected enum PGRelationKind {
        VertexProperty(Vertex.class),
        EdgeProperty(Edge.class),
        VertexMetaProperty(org.apache.tinkerpop.gremlin.structure.VertexProperty.class),
        Edge(Vertex.class);

        private final Class<? extends Element> domainClass;

        PGRelationKind(Class<? extends Element> domainClass) {
            this.domainClass = domainClass;
        }

        public Class<? extends Element> getDomainClass() {
            return domainClass;
        }
    }

    static {
        valueClassesByName = new HashMap<>();
        for (Vocabulary.BasicType entity : Vocabulary.BasicType.values()) {
            valueClassesByName.put(entity.getName(), entity.getJavaEquivalent());
        }
    }

    protected final Vocabulary vocabulary;

    protected final D database;

    private final Map<QualifiedName, StorageSchemaElement> elementsByQualifiedName = new HashMap<>();

    protected SchemaManager(final Vocabulary vocabulary,
                            final D database) {
        this.vocabulary = vocabulary;
        this.database = database;
    }

    /**
     * Gets the class of the database or storage schema management API
     */
    public D getDatabase() {
        return database;
    }

    protected abstract <T> IdempotentOperation addEntityType(StorageSchemaElement<EntityType, T> storageSchemaElement)
            throws InvalidSchemaException, UpdateFailedException;

    protected abstract <T> IdempotentOperation addRelationType(
            StorageSchemaElement<RelationType, T> storageSchemaElement)
            throws InvalidSchemaException, UpdateFailedException;

    protected abstract <E extends UpdateFailedException> void tryInTransaction(RunnableWithException<E> task) throws E;

    protected abstract boolean schemaExists();

    protected abstract IdempotentOperation addIndex(Index index) throws UpdateFailedException;

    protected abstract String nameForIndex(Index index);

    protected static <T> boolean isNonEmpty(final Iterable<T> test) {
        return test.iterator().hasNext();
    }

    protected static boolean isUniqueKey(final RelationType relationType) {
        return Cardinality.Part.One == relationType.getCardinality().getDomainPart();
    }

    protected static boolean isDataType(final Type type) {
        return type instanceof EntityType && ((EntityType) type).getIsDataType();
    }

    protected static Class getDataTypeClass(final EntityType entity) throws InvalidSchemaException {
        return valueClassesByName.get(entity.getSimpleBaseType().getLabel());
    }

    protected static PGRelationKind findLinkKind(final RelationType relationType) throws InvalidSchemaException {
        Type domain = relationType.getFrom();
        EntityType range = (EntityType) relationType.getTo();

        // note: vertex meta-properties are not yet supported
        if (domain instanceof RelationType) {
            return PGRelationKind.EdgeProperty;
        } else if (domain instanceof EntityType) {
            if (range.getIsDataType()) {
                return PGRelationKind.VertexProperty;
            } else {
                return PGRelationKind.Edge;
            }
        } else {
            throw new IllegalStateException();
        }
    }

    /**
     * Creates a database schema from the set of logical schemas provided to the constructor
     *
     * @throws UpdateFailedException if the database schema cannot be updated
     *                               (for example, if a schema element already exists)
     */
    public synchronized void initializeSchema(boolean isFreshSchema)
            throws UpdateFailedException {

        if (isFreshSchema) {
            checkForExistingSchema();
        }

        createDependencyTree();

        List<IdempotentOperation> workflow = new LinkedList<>();

        addTypesAndLinks(workflow);

        // add indexes after relationship types
        addIndexes(workflow);

        executeWorkflow(workflow);
    }

    /**
     * Applies a diff of logical schema changes to the live database
     *
     * @param diff a list of schema changes. Order may be significant.
     */
    public synchronized void applyDiff(final SchemaDiff diff) throws UpdateFailedException {
        List<IdempotentOperation> workflow = new LinkedList<>();

        for (SchemaChange change : diff.getChanges()) {
            workflow.add(toOperation(change));
        }

        executeWorkflow(workflow);
    }

    private IdempotentOperation toOperation(final SchemaChange change) throws UpdateFailedException {
        Object[] contextPath = change.getContextPath();
        switch (change.getType()) {
            case EntityTypeAdded:
                return handleTypeAdded(contextPath);
            case RelationTypeAdded:
                return handleLinkAdded(contextPath);
            case AbstractAttributeChanged:
                return handleAbstractAttributeChanged(contextPath);
            default:
                // for now, simply ignore changes not explicitly handled
                return IdempotentOperation.noop();
        }
    }

    private void executeWorkflow(final List<IdempotentOperation> workflow) throws UpdateFailedException {
        tryInTransaction(() -> {
            try {
                for (IdempotentOperation operation : workflow) {
                    operation.perform();
                }
            } catch (InvalidSchemaException e) {
                throw new InvalidUpdateException(e);
            }
        });
    }

    // TODO: mixed indexes, vertex-centric property indexes
    private void addIndexes(final List<IdempotentOperation> workflow)
            throws InvalidSchemaException, UpdateFailedException {
        for (Index idx : vocabulary.getIndexes()) {
            workflow.add(addIndex(idx));
        }
    }

    private void addTypesAndLinks(final List<IdempotentOperation> workflow)
            throws InvalidSchemaException, UpdateFailedException {
        List<StorageSchemaElement> sorted = sortElementsByDependencies();

        for (StorageSchemaElement storageSchemaElement : sorted) {
            if (storageSchemaElement.getSchemaElement().getAbstract()) {
                continue;
            }

            workflow.add(addEntity(storageSchemaElement));
        }
    }

    private IdempotentOperation addEntity(final StorageSchemaElement storageSchemaElement)
            throws UpdateFailedException {
        switch (storageSchemaElement.getTypeCategory()) {
            case Entity:
                return addEntityType(storageSchemaElement);
            case Relation:
                return addRelationType(storageSchemaElement);
            default:
                throw new IllegalStateException();
        }
    }

    private void createDependencyTree() throws InvalidSchemaException {
        for (EntityType el : vocabulary.getEntityTypes().values()) {
            toSchemaElement(el);
        }
        for (RelationType el : vocabulary.getRelationTypes().values()) {
            toSchemaElement(el);
        }
    }

    private void checkForExistingSchema() throws InvalidUpdateException {
        if (schemaExists()) {
            throw new InvalidUpdateException("storage schema already exists");
        }
    }

    private IdempotentOperation handleTypeAdded(final Object[] contextPath)
            throws InvalidSchemaException, UpdateFailedException {
        EntityType newEntityType = (EntityType) contextPath[contextPath.length - 1];
        if (!newEntityType.getAbstract()) {
            StorageSchemaElement<EntityType, ?> el = toSchemaElement(newEntityType);
            return addEntityType(el);
        } else {
            return IdempotentOperation.noop();
        }
    }

    private IdempotentOperation handleLinkAdded(final Object[] contextPath)
            throws InvalidSchemaException, UpdateFailedException {
        RelationType newRelationType = (RelationType) contextPath[contextPath.length - 1];
        if (!newRelationType.getAbstract()) {
            StorageSchemaElement<RelationType, ?> el = toSchemaElement(newRelationType);
            return addRelationType(el);
        } else {
            return IdempotentOperation.noop();
        }
    }

    private IdempotentOperation handleAbstractAttributeChanged(final Object[] contextPath)
            throws InvalidSchemaException, UpdateFailedException {
        Type newType = (Type) contextPath[contextPath.length - 1];
        // when an abstract entity becomes concrete, add it to the database schema
        if (!newType.getAbstract()) {
            if (newType instanceof EntityType) {
                return handleTypeAdded(contextPath);
            } else if (newType instanceof RelationType) {
                return handleLinkAdded(contextPath);
            } else {
                logger.warn("unexpected entity class: " + newType);
            }
        }

        return IdempotentOperation.noop();
    }

    private List<StorageSchemaElement> sortElementsByDependencies()
            throws InvalidSchemaException, UpdateFailedException {

        Stack<StorageSchemaElement> startNodes = new Stack<>();
        for (StorageSchemaElement storageSchemaElement : elementsByQualifiedName.values()) {
            if (0 == storageSchemaElement.getDependents().size()) {
                startNodes.add(storageSchemaElement);
            }
        }

        // topological sort
        List<StorageSchemaElement> sorted = new LinkedList<>();
        while (!startNodes.isEmpty()) {
            StorageSchemaElement el = startNodes.pop();
            sorted.add(el);
            List<StorageSchemaElement> deps = el.getDependencies();
            for (StorageSchemaElement dep : deps) {
                dep.getDependents().remove(el);
                if (0 == dep.getDependents().size()) {
                    startNodes.add(dep);
                }
            }
        }

        // extra check for cycles; validation checks should prevent this
        for (StorageSchemaElement el : elementsByQualifiedName.values()) {
            if (0 != el.getDependents().size()) {
                throw new InvalidSchemaException("dependency cycle(s) detected at " + el.getSchemaElement());
            }
        }

        return sorted;
    }

    protected <T extends Type> StorageSchemaElement getSchemaElement(final T element) {
        return elementsByQualifiedName.get(element.getName());
    }

    private <E extends Type, T> StorageSchemaElement<E, T> toSchemaElement(final E element)
            throws InvalidSchemaException {

        StorageSchemaElement<E, T> storageSchemaElement = getSchemaElement(element);
        if (null == storageSchemaElement && !element.getAbstract()) {
            storageSchemaElement = createSchemaElement(element);
            addSchemaElement(element, storageSchemaElement);
        }
        return storageSchemaElement;
    }

    private <E extends Type, T> void addSchemaElement(
            final E element,
            final StorageSchemaElement<E, T> storageSchemaElement) {
        elementsByQualifiedName.put(element.getName(), storageSchemaElement);
    }

    protected abstract <C extends Type, T> StorageSchemaElement<C, T> createSchemaElement(C element);

    protected interface RunnableWithException<E extends Exception> {
        void run() throws E;
    }

    public interface Loader {
        <T> T load(String path, Class<T> resourceClass) throws IOException;
    }

    public static class FileLoader implements Loader {
        private final File baseDirectory;

        public FileLoader(File baseDirectory) {
            this.baseDirectory = baseDirectory;
        }

        @Override
        public <T> T load(final String path, final Class<T> resourceClass) throws IOException {
            logger.info("loading file:" + path);
            File file = new File(baseDirectory, path);
            try (InputStream is = new FileInputStream(file)) {
                return (T) new Yaml(new Constructor(resourceClass)).load(is);
            }
        }
    }

    public static class ResourceLoader implements Loader {
        @Override
        public <T> T load(final String path, final Class<T> resourceClass) throws IOException {
            logger.info("loading resource:" + path);
            try (InputStream is = SchemaManager.class.getResourceAsStream(path)) {
                return (T) new Yaml(new Constructor(resourceClass)).load(is);
            }
        }
    }

    /**
     * An object which connects an element of the conceptual schema with an element of the storage schema
     *
     * @param <C> the class of the conceptual schema type (entity or relation)
     * @param <T> the class of the storage schema element
     */
    public abstract class StorageSchemaElement<C extends Type, T> {
        protected TypeCategory typeCategory;
        protected String storageName;
        protected C schemaElement;
        T storageElement;
        List<StorageSchemaElement> dependencies = new LinkedList<>();
        List<StorageSchemaElement> dependents = new LinkedList<>();

        protected StorageSchemaElement(C schemaElement) {
            this.schemaElement = schemaElement;
            if (schemaElement instanceof EntityType) {
                typeCategory = TypeCategory.Entity;
                asType((EntityType) schemaElement);
            } else if (schemaElement instanceof RelationType) {
                asLink((RelationType) schemaElement);
                Type domain = ((RelationType) schemaElement).getFrom();
                Type range = ((RelationType) schemaElement).getTo();
                addDependency(domain);
                addDependency(range);
            } else {
                throw new IllegalStateException();
            }
        }

        protected abstract void asType(EntityType entityType);

        protected abstract void asLink(RelationType relationType);

        /**
         * Gets the category (entity or relation) of the conceptual schema type
         */
        public TypeCategory getTypeCategory() {
            return typeCategory;
        }

        /**
         * Gets the name of the storage schema element
         */
        public String getStorageName() {
            return storageName;
        }

        /**
         * Gets the conceptual schema element
         */
        public C getSchemaElement() {
            return schemaElement;
        }

        /**
         * Gets the storage schema element
         */
        public T getStorageElement() {
            return storageElement;
        }

        /**
         * Sets the storage element
         */
        public void setStorageElement(final T storageElement) {
            this.storageElement = storageElement;
        }

        /**
         * Gets the storage schema element's dependencies (other elements it should be defined after)
         */
        List<StorageSchemaElement> getDependencies() {
            return dependencies;
        }

        /**
         * Gets the storage schema element's dependents (other elements it should be defined before)
         */
        public List<StorageSchemaElement> getDependents() {
            return dependents;
        }

        <E extends Type> void addDependency(final E element) throws InvalidSchemaException {
            StorageSchemaElement<E, T> storageSchemaElement = toSchemaElement(element);
            if (null != storageSchemaElement) {
                dependencies.add(storageSchemaElement);
                storageSchemaElement.dependents.add(this);
            }
        }
    }

    /**
     * An exception which is thrown when a valid update operation fails, for any reason
     */
    @SuppressWarnings("serial")
    public static class UpdateFailedException extends Exception {
        public UpdateFailedException(final String message, final Throwable cause) {
            super(message, cause);
        }

        UpdateFailedException(final String message) {
            super(message);
        }

        UpdateFailedException(final Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception which is thrown when an update operation is found to be invalid/illegal
     */
    @SuppressWarnings("serial")
    public static class InvalidUpdateException extends UpdateFailedException {
        public InvalidUpdateException(final String message, final Throwable cause) {
            super(message, cause);
        }

        InvalidUpdateException(final String message) {
            super(message);
        }

        InvalidUpdateException(final Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception which is thrown when a valid add operation fails
     */
    @SuppressWarnings("serial")
    static class AddFailedException extends UpdateFailedException {
        AddFailedException(Object missingElement) {
            super("element was not created in the database: " + missingElement);
        }
    }
}
