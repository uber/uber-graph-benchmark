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

package com.uber.ugb.db;

/**
 * The result of an operation.
 */
public class Status {

    public static final Status OK = new Status("OK", "The operation completed successfully.");
    public static final Status ERROR = new Status("ERROR", "The operation failed.");
    public static final Status NOT_FOUND = new Status("NOT_FOUND", "The requested record was not found.");
    public static final Status NOT_IMPLEMENTED = new Status("NOT_IMPLEMENTED", "The operation is not "
        + "implemented for the current binding.");
    public static final Status UNEXPECTED_STATE = new Status("UNEXPECTED_STATE", "The operation reported"
        + " success, but the result was not as expected.");
    public static final Status BAD_REQUEST = new Status("BAD_REQUEST", "The request was not valid.");

    private final String name;
    private final String description;

    /**
     * @param name        A short name for the status.
     * @param description A description of the status.
     */
    public Status(String name, String description) {
        super();
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "Status [name=" + name + ", description=" + description + "]";
    }

    /**
     * Is {@code this} a passing state for the operation: {@link Status#OK}.
     *
     * @return true if the operation is successful, false otherwise
     */
    public boolean isOk() {
        return this == OK;
    }

}
