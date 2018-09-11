package com.uber.ugb.db;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.uber.ugb.measurement.Metrics;
import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.Vocabulary;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DB {

    protected Vocabulary vocabulary;
    /**
     * Properties for configuring this DB.
     */
    private Properties properties = new Properties();

    private Metrics metrics;

    protected static Properties extractProperties(Properties properties, String select, String filterField) {
        if (Strings.isNullOrEmpty(select)) {
            return properties;
        }
        Properties answer = new Properties();
        AtomicBoolean hasFilterField = new AtomicBoolean();
        Splitter.on(',').omitEmptyStrings().trimResults().split(select).forEach(s -> {
            if (s.equals(filterField)) {
                hasFilterField.set(true);
            }
            if (properties.containsKey(s)) {
                answer.put(s, properties.getProperty(s));
            }
        });
        if (!hasFilterField.get() && filterField != null) {
            if (properties.containsKey(filterField)) {
                answer.put(filterField, properties.getProperty(filterField));
            }
        }
        return answer;
    }

    public Vocabulary getVocabulary() {
        return vocabulary;
    }

    public void setVocabulary(Vocabulary vocabulary) {
        this.vocabulary = vocabulary;
    }

    public Metrics getMetrics() {
        if (metrics == null) {
            metrics = new Metrics();
        }
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException {
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException {
    }

    public abstract Status writeVertex(String label, Object id, Object... keyValues);

    public abstract Status writeEdge(String edgeLabel,
                                     String outVertexLabel, Object outVertexId, String inVertexLabel, Object inVertexId,
                                     Object... keyValues);

    public abstract Status subgraph(QueriesSpec.Query query, Subgraph subgraph);

    public Status commitBatch() {
        return Status.OK;
    }

    /**
     * Get the set of properties for this DB.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Set the properties for this DB.
     */
    public void setProperties(Properties p) {
        properties = p;
    }

    /**
     * genVertexId customizable way to generate a vertex id
     *
     * @param label
     * @param id
     * @return
     */
    public Object genVertexId(String label, long id) {
        return UUID.nameUUIDFromBytes((label + id).getBytes()).getLeastSignificantBits();
    }

}
