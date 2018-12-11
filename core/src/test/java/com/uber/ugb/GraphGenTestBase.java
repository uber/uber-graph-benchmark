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

package com.uber.ugb;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.function.Consumer;

public abstract class GraphGenTestBase {

    protected void writeTo(final File file, final Consumer<PrintStream> task) throws IOException {
        try (OutputStream out = new FileOutputStream(file)) {
            PrintStream ps = new PrintStream(out);
            task.accept(ps);
        }
    }

    protected long countVertices(final Graph graph) {
        return graph.traversal().V().count().next();
    }

    protected File createTempDir() throws IOException {
        File file = File.createTempFile("test", "dir");
        file.deleteOnExit();
        file.delete();
        file.mkdirs();
        return file;
    }
}
