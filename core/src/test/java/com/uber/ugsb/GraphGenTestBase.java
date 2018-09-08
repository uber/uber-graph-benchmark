package com.uber.ugsb;

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
