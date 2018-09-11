package com.uber.ugb.schema.model.dto;

import com.uber.ugb.schema.Vocabulary;
import com.uber.ugb.schema.model.SchemaElement;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * A lightweight object representing an index hint. These DTOs are used for schema construction and serialization.
 * See <code>Index</code> for the materialized form of this class.
 */
public class IndexDTO extends SchemaElement {
    private static final long serialVersionUID = Vocabulary.serialVersionUID;

    private String key;
    private Direction direction = Direction.OUT;
    private Order order = Order.incr;
    private String orderBy;

    public IndexDTO() {
    }

    public IndexDTO(final String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }
}
