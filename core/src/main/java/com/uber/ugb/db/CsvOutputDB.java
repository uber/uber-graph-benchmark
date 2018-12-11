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

import com.uber.ugb.queries.QueriesSpec;
import com.uber.ugb.schema.QualifiedName;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

/*
 * CsvOutputDB writes to System.out or a file defined in env.properties via property "csv.output".
 */
public class CsvOutputDB extends DB {

    private PrintStream out;

    @Override
    public void init() throws DBException {
        String outputFile = this.getProperties().getProperty("csv.output", null);
        if (outputFile == null) {
            out = System.out;
        } else {
            try {
                out = new PrintStream(new FileOutputStream(outputFile));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                out = System.out;
            }
        }
        return;
    }

    @Override
    public void cleanup() throws DBException {
        out.close();
        return;
    }

    @Override
    public Status writeVertex(QualifiedName label, Object id, Object... keyValues) {
        synchronized (out){
            out.print("v:");
            out.print(label);
            out.print(",");
            out.print(id);
            for (int i = 0; i < keyValues.length - 1; i += 2) {
                out.print(",");
                out.print(keyValues[i + 1]);
            }
            out.println();
        }
        return Status.OK;
    }

    @Override
    public Status writeEdge(QualifiedName edgeLabel,
                            QualifiedName outVertexLabel, Object outVertexId,
                            QualifiedName inVertexLabel, Object inVertexId,
                            Object... keyValues) {
        synchronized (out){
            out.print("e:");
            out.print(edgeLabel);
            out.print(",");
            out.print(outVertexLabel);
            out.print(",");
            out.print(outVertexId);
            out.print(",");
            out.print(inVertexLabel);
            out.print(",");
            out.print(inVertexId);
            for (int i = 0; i < keyValues.length - 1; i += 2) {
                out.print(",");
                out.print(keyValues[i + 1]);
            }
            out.println();
        }
        return Status.OK;
    }

    @Override
    public Status subgraph(QueriesSpec.Query query, Subgraph subgraph) {
        return Status.NOT_IMPLEMENTED;
    }

}
