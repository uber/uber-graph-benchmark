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

/**
 * An operation which, when executed twice, has the same effect as when it is executed only once
 *
 * @param <N>
 * @param <G>
 */
public abstract class IdempotentOperation<N, G> {
    protected final G graphElement;

    protected IdempotentOperation(G graphElement) {
        this.graphElement = graphElement;
    }

    protected abstract void execute() throws SchemaManager.UpdateFailedException, InvalidSchemaException;

    protected abstract N getExisting();

    protected abstract void checkExistingIsAsExpected(N existing)
            throws InvalidSchemaException, SchemaManager.InvalidUpdateException;

    private void checkCorrectlyExecuted()
            throws SchemaManager.AddFailedException, InvalidSchemaException, SchemaManager.InvalidUpdateException {
        N existing = getExisting();
        if (null == existing) {
            throw new SchemaManager.AddFailedException(graphElement);
        }
        checkExistingIsAsExpected(existing);
    }

    /**
     * Executes the operation
     */
    public void perform() throws SchemaManager.UpdateFailedException, InvalidSchemaException {
        if (isNecessary()) {
            execute();
            checkCorrectlyExecuted();
        }
    }

    private boolean isNecessary() throws SchemaManager.UpdateFailedException, InvalidSchemaException {
        N existing = getExisting();
        if (null == existing) {
            return true;
        }

        checkExistingIsAsExpected(existing);

        return false;
    }

    protected void checkArgument(final boolean condition) throws SchemaManager.InvalidUpdateException {
        if (!condition) {
            throw new SchemaManager.InvalidUpdateException("failed to create element " + graphElement);
        }
    }

    /**
     * A no-op; this operation does nothing.
     */
    public static IdempotentOperation noop() {
        return new IdempotentOperation<String, String>("noop") {
            @Override
            protected void execute() throws SchemaManager.UpdateFailedException, InvalidSchemaException {
            }

            @Override
            protected String getExisting() {
                return "noop";
            }

            @Override
            protected void checkExistingIsAsExpected(String existing)
                    throws InvalidSchemaException, SchemaManager.InvalidUpdateException {
            }
        };
    }
}
