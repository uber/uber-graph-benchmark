package com.uber.ugb.measurement;

import java.io.IOException;

public interface Measurement {

    boolean hasData();

    void measure(long latencyNs);

    void measure(Runnable runnable);

    void printout(MetricsOutput out) throws IOException;
}
