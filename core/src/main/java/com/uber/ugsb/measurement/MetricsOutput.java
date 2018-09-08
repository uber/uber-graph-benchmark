package com.uber.ugsb.measurement;

import java.io.IOException;

/**
 * print out metrics in text or json format
 */
public interface MetricsOutput {

    void write(String category, String metric, long i) throws IOException;

    void write(String category, String metric, double d) throws IOException;

}
