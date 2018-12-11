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

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static org.junit.Assert.assertTrue;

/**
 * Miscellaneous utilities for use with schemas and ingestion
 */
public final class SchemaUtils {
    /**
     * Creates a directory which will be deleted on exit
     */
    public static File createTempDirectory() throws IOException {
        File tmpFile = File.createTempFile("ugraph-tmp-", "-dir");
        tmpFile.delete();
        tmpFile.mkdirs();
        assertTrue(tmpFile.exists());
        assertTrue(tmpFile.isDirectory());
        tmpFile.deleteOnExit();
        return tmpFile;
    }

    /**
     * Creates a shallow copy of a directory in a new location
     */
    public static void copyDirectory(final Path src, final Path dest) throws IOException {
        Files.walk(src).forEach(s -> {
            try {
                Path d = dest.resolve(src.relativize(s));
                if (Files.isDirectory(s)) {
                    if (!Files.exists(d)) {
                        Files.createDirectory(d);
                    }
                    return;
                }
                Files.copy(s, d);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * Converts from lowerCamelCase or UpperCamelCase to UPPER_SNAKE_CASE
     */
    public static String toUpperSnakeCase(final String camelCase) {
        StringBuilder sb = new StringBuilder();
        char[] chars = camelCase.toCharArray();
        boolean lastUpper = Character.isUpperCase(chars[0]);
        boolean lastAlph = Character.isAlphabetic(chars[0]);
        for (char c : chars) {
            boolean isNum = Character.isDigit(c);
            boolean isAlph = Character.isAlphabetic(c);
            boolean isUpper = Character.isUpperCase(c);
            if (c != '_') {
                if (isNum) {
                    isAlph = lastAlph;
                    isUpper = lastUpper;
                } else {
                    if (lastAlph != isAlph || (isUpper && !lastUpper)) {
                        sb.append("_");
                    }
                }
                sb.append(Character.toUpperCase(c));
            }
            lastAlph = isAlph;
            lastUpper = isUpper;
        }
        return sb.toString();
    }

    /**
     * Converts from lowerCamelCase or UpperCamelCase to lower_snake_case
     */
    public static String toLowerSnakeCase(final String camelCase) {
        return toUpperSnakeCase(camelCase).toLowerCase();
    }

    /**
     * Converts from lower_snake_case or UPPER_SNAKE_CASE to UpperCamelCase
     */
    public static String toUpperCamelCase(final String snakeCase) {
        StringBuilder sb = new StringBuilder();
        char[] chars = snakeCase.toCharArray();
        boolean isUpper = true;
        for (char c : chars) {
            if (c == '_') {
                isUpper = true;
            } else {
                char out = isUpper ? Character.toUpperCase(c) : Character.toLowerCase(c);
                isUpper = false;
                sb.append(out);
            }
        }
        return sb.toString();
    }

    /**
     * Converts from lower_snake_case or UPPER_SNAKE_CASE to lowerCamelCase
     */
    public static String toLowerCamelCase(final String snakeCase) {
        if (snakeCase.indexOf("_") < 0 && Character.isLowerCase(snakeCase.charAt(0))) {
            return snakeCase;
        }
        String camelCase = toUpperCamelCase(snakeCase);
        return camelCase.substring(0, 1).toLowerCase() + camelCase.substring(1);
    }

    public static String lowerSnakeCaseToLowerCamelCase(String key) {
        return LOWER_UNDERSCORE.to(LOWER_CAMEL, key);
    }

    public static String lowerCamelCaseToLowerSnakeCase(String key) {
        return LOWER_CAMEL.to(LOWER_UNDERSCORE, key);
    }

    /**
     * Creates an empty in-memory graph
     */
    public static TinkerGraph createTinkerGraph() {
        return TinkerGraph.open();
    }

    /**
     * Determines whether there is a cycle in a relationship defined by a function from an object to a collection
     *
     * @param start       the start node of the potential cycle
     * @param getAdjacent a function to get the nodes reachable from the start node
     */
    public static <T, X extends Exception> boolean hasCycle(
        T start, FunctionWithException<T, Iterable<T>, X> getAdjacent) throws X {
        return hasCycleInternal(start, new HashSet<T>(), new HashSet<T>(), getAdjacent);
    }

    private static <T, X extends Exception> boolean hasCycleInternal(
        T start, Set<T> visited, Set<T> work, FunctionWithException<T, Iterable<T>, X> getChildren) throws X {
        if (work.contains(start)) {
            return true;
        } else if (visited.contains(start)) {
            return false;
        } else {
            visited.add(start);
            Iterable<T> children = getChildren.apply(start);
            if (null != children) {
                work.add(start);
                for (T child : getChildren.apply(start)) {
                    if (hasCycleInternal(child, visited, work, getChildren)) {
                        return true;
                    }
                }
                work.remove(start);
            }
            return false;
        }
    }

    public interface FunctionWithException<D, R, X extends Throwable> {
        R apply(D d) throws X;
    }
}
