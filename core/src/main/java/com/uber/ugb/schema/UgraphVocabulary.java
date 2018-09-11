package com.uber.ugb.schema;

import com.uber.ugb.schema.model.EntityType;
import com.uber.ugb.schema.model.RelationType;
import com.uber.ugb.schema.model.Schema;
import com.uber.ugb.schema.model.dto.SchemaDTO;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Constants and helper methods for the uGraph core schemas
 */
public class UgraphVocabulary extends Vocabulary {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

    /**
     * An enumeration of the schemas comprising the uGraph vocabulary
     */
    public enum CoreSchema {
        CORE("core"),
        DEVICES("devices"),
        DOCUMENTS("documents"),
        REFERRALS("referrals"),
        TRIPS("trips"),
        USERS("users"),
        VEHICLES("vehicles");

        private final String schemaName;

        CoreSchema(final String schemaName) {
            this.schemaName = schemaName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        /**
         * Gets an input stream to read the YAML document defining any uGraph schema
         */
        public InputStream getYamlInputStream() {
            return getGraphSchemaInputStream(schemaName);
        }

        /**
         * Copies into a designated directory the YAML document defining any uGraph schema
         */
        public void copyTo(final File destDirectory) throws IOException {
            try (InputStream input = getYamlInputStream()) {
                File file = new File(destDirectory, getSchemaName() + ".yaml");
                try (OutputStream output = new FileOutputStream(file)) {
                    IOUtils.copy(input, output);
                }
            }
        }
    }

    /**
     * A partial enumeration of vertex (structured entity) types in the uGraph vocabulary
     */
    public enum VertexType {
        DOCUMENT("documents.Document"),
        LEGACY_DEVICE("devices.LegacyDevice"),
        TRIP("trips.Trip"),
        USER("users.User");

        static {
            stringToType = new HashMap<>();
            addTypes();
        }

        private static final Map<String, VertexType> stringToType;
        private final String label;
        private final EntityType type;

        VertexType(final String name) {
            QualifiedName qName = new QualifiedName(name);
            label = qName.getLocalName();
            type = getInstance().getEntityType(qName);
        }

        public String getLabel() {
            return label;
        }

        @Override
        public String toString() {
            return this.label;
        }

        public EntityType getType() {
            return type;
        }

        public static VertexType getType(final String name) {
            return stringToType.getOrDefault(name, null);
        }

        private static void addTypes() {
            for (VertexType type : VertexType.values()) {
                stringToType.put(type.toString(), type);
            }
        }
    }

    /**
     * A partial enumeration of edge (structured relation) types in the uGraph vocabulary
     */
    public enum EdgeType {
        DROVE_FOR("trips.droveFor"),
        REFERRED_DRIVER("referrals.referredDriver"),
        REFERRED_EATER("referrals.referredEater"),
        REFERRED_RIDER("referrals.referredRider"),
        REQUESTED("trips.requested"),
        USED_DOCUMENT("documents.usedDocument");

        private final RelationType type;

        EdgeType(final String name) {
            this.type = UgraphVocabulary.getInstance().getRelationType(new QualifiedName(name));
        }

        public String getLabel() {
            return name();
        }

        public RelationType getType() {
            return type;
        }
    }

    /**
     * A partial enumeration of vertex property (entity datatype-valued) types in the uGraph vocabulary
     */
    public enum VertexPropertyType {
        CITY_ID("trips.cityId"),
        CONTACT_NUMBER("users.contactNumber"),
        COUNTRY_ISO2_CODE("documents.countryIso2Code"),
        CREATED_AT("users.createdAt"),
        DOCUMENT_ID("documents.documentId"),
        DOCUMENT_TYPE("documents.documentType"),
        EMAIL("users.email"),
        IS_DRIVER("users.isDriver"),
        IS_DELETED("core.isDeletedEntity"),
        MODEL("devices.model");

        private final RelationType type;

        VertexPropertyType(final String name) {
            this.type = UgraphVocabulary.getInstance().getRelationType(new QualifiedName(name));
        }

        public String getLabel() {
            return type.getLabel();
        }

        public RelationType getType() {
            return type;
        }
    }

    /**
     * A partial enumeration of edge property (relation datatype-valued) types in the uGraph vocabulary
     */
    public enum EdgePropertyType {
        CREATED_AT("core.createdAt"),
        IS_DELETED("core.isDeletedRelation");

        private final RelationType type;

        EdgePropertyType(final String name) {
            this.type = UgraphVocabulary.getInstance().getRelationType(new QualifiedName(name));
        }

        public String getLabel() {
            return type.getLabel();
        }

        public RelationType getType() {
            return type;
        }
    }

    private static final Vocabulary instance;

    static {
        logger = LoggerFactory.getLogger(UgraphVocabulary.class);

        try {
            instance = new UgraphVocabulary(loadSchemas());
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final Logger logger;

    private UgraphVocabulary(Collection<Schema> schemasToAdd) throws InvalidSchemaException {
        super(schemasToAdd);
    }

    /**
     * Gets the singleton instance of this vocabulary
     */
    public static Vocabulary getInstance() {
        return instance;
    }

    static void loadGraphSchema(final String schemaName, final SchemaBuilder builder)
            throws IOException {
        try (InputStream in = getGraphSchemaInputStream(schemaName)) {
            SchemaDTO schema = builder.addSchema(in);
            for (String other : schema.getIncludes()) {
                if (!builder.getSchemaDTOs().containsKey(other)) {
                    loadGraphSchema(other, builder);
                }
            }
        }
    }

    static void loadSchemas(final SchemaBuilder builder) throws IOException {
        try (InputStream in = getIndexingSchemaInputStream()) {
            builder.addSchema(in);
            for (CoreSchema schema : CoreSchema.values()) {
                loadGraphSchema(schema.getSchemaName(), builder);
            }
        }
    }

    private static InputStream getGraphSchemaInputStream(final String schemaName) {
        return getResourceInputStream("/concepts/" + schemaName + ".yaml");
    }

    private static List<Schema> loadSchemas() throws IOException, InvalidSchemaException {
        SchemaBuilder builder = new SchemaBuilder();
        loadSchemas(builder);
        return builder.toVocabulary().getSchemas();
    }

    private static InputStream getResourceInputStream(final String path) {
        logger.info("loading schema at " + path);
        InputStream inputStream = UgraphVocabulary.class.getResourceAsStream(path);
        if (null == inputStream) {
            throw new IllegalStateException("no resource at path " + path);
        }
        return inputStream;
    }

    private static InputStream getIndexingSchemaInputStream() {
        return getResourceInputStream("/production.yaml");
    }

    // TODO @joshsh: move these?
    public static final class GraphElements {
        public static final String DEFAULT = "default";
        public static final String SOURCE = "source";
        public static final String CLIENT = "client";
        public static final String UUID = "uuid";
    }
}
