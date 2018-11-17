package com.uber.ugb.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class ProgressReporter {

    private static Logger logger = Logger.getLogger(ProgressReporter.class.getName());

    String prefix;
    AtomicLong start;
    AtomicLong stop;
    boolean verbose;
    AtomicLong lastLogTime;
    AtomicLong lastCount;
    long countInterval;

    public ProgressReporter(String prefix, long start, long stop, long countInterval) {
        this.prefix = prefix;
        this.start = new AtomicLong(start);
        this.stop = new AtomicLong(stop);
        this.verbose = true;
        this.lastLogTime = new AtomicLong(System.currentTimeMillis());
        this.lastCount = new AtomicLong();
        this.countInterval = countInterval;
    }

    public void maybeReport(long count) {
        if (verbose) {
            this.report(count);
        }
    }

    public void report(long count) {
        long now = System.currentTimeMillis();
        long deltaTime = now - lastLogTime.get();
        if (deltaTime > 2000L) {
            logger.info(String.format("%s:%d/[%d,%d) %.1fk/s %.2f%%",
                prefix,
                count, start.get(), stop.get(),
                (count - lastCount.get()) / (float) (deltaTime),
                ((count-start.get()) * 100.0f / (stop.get()-start.get()))
            ));
            lastLogTime.set(now);
            lastCount.set(count);
        }
    }
}
