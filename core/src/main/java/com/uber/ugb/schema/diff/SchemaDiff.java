package com.uber.ugb.schema.diff;

import com.uber.ugb.schema.Vocabulary;
import com.uber.ugb.schema.model.EntityType;
import com.uber.ugb.schema.model.Index;
import com.uber.ugb.schema.model.RelationType;
import com.uber.ugb.schema.model.Schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A semantic diff between two versions of a schema.
 * The diff is a collection of basic changes such as addition or removal of an entity or relation type,
 * change of relation cardinality, etc.
 */
public class SchemaDiff {

    private final List<SchemaChange> changes;

    /**
     * Finds a list of differences between the given vocabularies.
     *
     * @param first  the first vocabulary
     * @param second a second vocabulary.
     *               Schemas not present in the first vocabulary and present in the second vocabulary
     *               are captured as additions, and conversely for deletions.
     *               For those schemas present in both vocabularies, per-schema changes are found.
     */
    public SchemaDiff(final Vocabulary first, final Vocabulary second) {
        changes = new LinkedList<>();
        Map<String, Schema> firstSchemas = getSchemasByName(first);
        Map<String, Schema> secondSchemas = getSchemasByName(second);
        Set<String> schemasRemoved = new HashSet<>();
        Set<String> schemasAdded = new HashSet<>();
        for (String key : firstSchemas.keySet()) {
            if (!secondSchemas.containsKey(key)) {
                schemasRemoved.add(key);
                changes.add(new SchemaChange(SchemaChange.Type.SchemaRemoved, firstSchemas.get(key)));
            } else {
                SchemaDiffHelper helper = new SchemaDiffHelper(firstSchemas.get(key), secondSchemas.get(key), changes);
                helper.addDiff();
            }
        }
        for (String key : secondSchemas.keySet()) {
            if (!firstSchemas.containsKey(key)) {
                schemasAdded.add(key);
                changes.add(new SchemaChange(SchemaChange.Type.SchemaAdded, secondSchemas.get(key)));
            }
        }
        for (String key : schemasRemoved) {
            Schema schema = firstSchemas.get(key);
            for (EntityType entityType : schema.getEntityTypes().values()) {
                changes.add(new SchemaChange(SchemaChange.Type.EntityTypeRemoved, entityType));
            }
            for (RelationType relationType : schema.getRelationTypes().values()) {
                changes.add(new SchemaChange(SchemaChange.Type.RelationTypeRemoved, relationType));
            }
            for (Index index : schema.getIndexes()) {
                changes.add(new SchemaChange(SchemaChange.Type.IndexRemoved, index));
            }
        }
        for (String key : schemasAdded) {
            Schema schema = secondSchemas.get(key);
            for (EntityType entityType : schema.getEntityTypes().values()) {
                changes.add(new SchemaChange(SchemaChange.Type.EntityTypeAdded, entityType));
            }
            for (RelationType relationType : schema.getRelationTypes().values()) {
                changes.add(new SchemaChange(SchemaChange.Type.RelationTypeAdded, relationType));
            }
            for (Index index : schema.getIndexes()) {
                changes.add(new SchemaChange(SchemaChange.Type.IndexAdded, index));
            }
        }
    }

    /**
     * Finds a list of differences between the given schemas.
     *
     * @param first  the first schema
     * @param second a second schema. Elements not present in the first schema and present in the second schema
     *               are captured as additions, and conversely for deletions.
     */
    public SchemaDiff(final Schema first, final Schema second) {
        changes = new LinkedList<>();
        SchemaDiffHelper helper = new SchemaDiffHelper(first, second, changes);
        helper.addDiff();
    }

    /**
     * An ordered list of all changes in this diff. It should be possible, starting with an first version of
     * the schema vocabulary, to apply each change in turn to arrive at the second version.
     */
    public List<SchemaChange> getChanges() {
        return changes;
    }

    /**
     * Gets a human-readable summary of a schema diff, as a string
     */
    public String getSummary() {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        sb.append(changes.size()).append(" changes\n");
        for (SchemaChange change : changes) {
            if (first) {
                first = false;
            } else {
                sb.append("\n");
            }
            sb.append("\t").append(change);
        }
        return sb.toString();
    }

    private Map<String, Schema> getSchemasByName(final Vocabulary vocabulary) {
        Map<String, Schema> map = new HashMap<>();
        for (Schema schema : vocabulary.getSchemas()) {
            map.put(schema.getName(), schema);
        }
        return map;
    }

    private static class SchemaDiffHelper {
        private final Schema oldSchema;
        private final Schema newSchema;
        private final List<SchemaChange> changes;

        SchemaDiffHelper(final Schema oldSchema, final Schema newSchema, final List<SchemaChange> changes) {
            this.oldSchema = oldSchema;
            this.newSchema = newSchema;
            this.changes = changes;
        }

        void addDiff() {
            checkNameAndVersion();
            checkImports();
            checkTypes();
            checkLinks();
            checkIndexes();
        }

        private void checkNameAndVersion() {
            if (!newSchema.getName().equals(oldSchema.getName())) {
                addChange(SchemaChange.Type.SchemaNameChanged, oldSchema);
            }
        }

        private void addChange(final SchemaChange.Type type, Object... contextPath) {
            changes.add(new SchemaChange(type, contextPath));
        }

        private void checkImports() {
            addDiff(oldSchema.getIncludes(), newSchema.getIncludes(),
                    (ref -> new SchemaChange(SchemaChange.Type.IncludeAdded, oldSchema, ref)),
                    (ref -> new SchemaChange(SchemaChange.Type.IncludeRemoved, oldSchema, ref)),
                    this::ignorePair);
        }

        private void checkTypes() {
            addDiff(oldSchema.getEntityTypes().values(), newSchema.getEntityTypes().values(),
                    (type -> new SchemaChange(SchemaChange.Type.EntityTypeAdded, oldSchema, type)),
                    (type -> new SchemaChange(SchemaChange.Type.EntityTypeRemoved, oldSchema, type)),
                    this::checkTypePair);
        }

        private void checkLinks() {
            addDiff(oldSchema.getRelationTypes().values(), newSchema.getRelationTypes().values(),
                    (type -> new SchemaChange(SchemaChange.Type.RelationTypeAdded, oldSchema, type)),
                    (type -> new SchemaChange(SchemaChange.Type.RelationTypeRemoved, oldSchema, type)),
                    this::checkLinkPair);
        }

        private void checkIndexes() {
            // note: if there is any change to an index, it is considered to be a new index
            addDiff(oldSchema.getIndexes(), newSchema.getIndexes(),
                    (index -> new SchemaChange(SchemaChange.Type.IndexAdded, oldSchema, index)),
                    (index -> new SchemaChange(SchemaChange.Type.IndexRemoved, oldSchema, index)),
                    this::ignorePair);
        }

        private <T> void ignorePair(final T first, final T second) {
        }

        private void checkTypePair(final EntityType first, final EntityType second) {
            addDiff(first.getExtends(), second.getExtends(),
                    (ext -> new SchemaChange(SchemaChange.Type.ExtensionAdded, oldSchema, second, ext)),
                    (ext -> new SchemaChange(SchemaChange.Type.ExtensionRemoved, oldSchema, first, ext)),
                    this::ignorePair);

            if (first.getAbstract() ^ second.getAbstract()) {
                addChange(SchemaChange.Type.AbstractAttributeChanged, oldSchema, second);
            }

            // TODO: enumerated values changed
        }

        private void checkLinkPair(final RelationType first, final RelationType second) {
            addDiff(first.getExtends(), second.getExtends(),
                    (ext -> new SchemaChange(SchemaChange.Type.ExtensionAdded, oldSchema, second, ext)),
                    (ext -> new SchemaChange(SchemaChange.Type.ExtensionAdded, oldSchema, first, ext)),
                    this::ignorePair);

            if (first.getAbstract() ^ second.getAbstract()) {
                addChange(SchemaChange.Type.AbstractAttributeChanged, oldSchema, second);
            }

            if (first.getRequired() ^ second.getRequired()) {
                addChange(SchemaChange.Type.RequiredAttributeChanged, oldSchema, first);
            }

            if (first.getRequiredOf() ^ second.getRequiredOf()) {
                addChange(SchemaChange.Type.RequiredOfAttributeChanged, oldSchema, first);
            }

            if (!Objects.equals(first.getCardinality(), second.getCardinality())) {
                addChange(SchemaChange.Type.CardinalityChanged, oldSchema, first);
            }

            if (!Objects.equals(first.getFrom(), second.getFrom())) {
                addChange(SchemaChange.Type.DomainChanged, oldSchema, first);
            }
            if (!Objects.equals(first.getTo(), second.getTo())) {
                addChange(SchemaChange.Type.RangeChanged, oldSchema, first);
            }
        }

        private <R> void addDiff(final Collection<R> firstSet,
                                 final Collection<R> secondSet,
                                 final Function<R, SchemaChange> addedConstructor,
                                 final Function<R, SchemaChange> removedConstructor,
                                 final BiConsumer<R, R> elementDiffFunction) {
            SetDiff<R> diff = new SetDiff<>(firstSet, secondSet);

            // elements only in one or the other
            for (R item : diff.onlyInFirst) {
                changes.add(removedConstructor.apply(item));
            }
            for (R item : diff.onlyInSecond) {
                changes.add(addedConstructor.apply(item));
            }

            // shared elements
            for (R first : diff.firstSet.keySet()) {
                if (!diff.onlyInFirst.contains(first)) {
                    R second = diff.secondSet.get(first);
                    elementDiffFunction.accept(first, second);
                }
            }
        }
    }

    private static class SetDiff<T> {
        private final Map<T, T> firstSet;
        private final Map<T, T> secondSet;
        private final Set<T> onlyInFirst = new HashSet<>();
        private final Set<T> onlyInSecond = new HashSet<>();

        SetDiff(Collection<T> firstSet, Collection<T> secondSet) {
            this(toSet(firstSet), toSet(secondSet));
        }

        private SetDiff(Map<T, T> firstSet, Map<T, T> secondSet) {
            this.firstSet = firstSet;
            this.secondSet = secondSet;

            for (T item : firstSet.keySet()) {
                if (!secondSet.keySet().contains(item)) {
                    onlyInFirst.add(item);
                }
            }

            for (T item : secondSet.keySet()) {
                if (!firstSet.keySet().contains(item)) {
                    onlyInSecond.add(item);
                }
            }
        }

        private static <T> Map<T, T> toSet(Collection<T> items) {
            Map<T, T> set = new HashMap<>();
            if (null != items) {
                for (T item : items) {
                    set.put(item, item);
                }
            }
            return set;
        }
    }
}
