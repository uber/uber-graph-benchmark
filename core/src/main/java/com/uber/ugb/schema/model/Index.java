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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Objects;

/**
 * An index hint. Currently used only with the JanusGraph back end.
 */
public class Index extends SchemaElement {
    private static final long serialVersionUID = -1131841444194436074L;

    private final RelationType relationType;
    private RelationType orderBy;
    private Direction direction;
    private Order order;

    /**
     * Constructs an index hint for the given relation type
     */
    public Index(RelationType relationType) {
        this.relationType = relationType;
    }

    /**
     * Gets the indexed relation type
     */
    public RelationType getRelationType() {
        return relationType;
    }

    /**
     * Gets the ordering constraint of the index, if any
     */
    public RelationType getOrderBy() {
        return orderBy;
    }

    /**
     * Sets the ordering constraint of the index.
     */
    public void setOrderBy(RelationType orderBy) {
        this.orderBy = orderBy;
    }

    /**
     * Gets the direction constraint of the index
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * Sets the direction constraint of the index
     */
    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    /**
     * Gets the ordering constraint of the index
     */
    public Order getOrder() {
        return order;
    }

    /**
     * Sets the ordering constraint of the index
     */
    public void setOrder(Order order) {
        this.order = order;
    }

    @Override
    public int hashCode() {
        return Objects.hash(relationType, orderBy, direction, order);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof Index
                && Objects.equals(relationType, ((Index) other).getRelationType())
                && Objects.equals(orderBy, ((Index) other).getOrderBy())
                && Objects.equals(direction, ((Index) other).getDirection())
                && Objects.equals(order, ((Index) other).getOrder());
    }

    @Override
    public String toString() {
        return "Index[" + relationType.getName() + "]";
    }
}
