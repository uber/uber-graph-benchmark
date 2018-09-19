package com.uber.ugb.model.generator;

public class UnixTimeMsGenerator extends Generator<Long> {

    private long base = 1537310687576L;
    private long tenYears = 10 * 35 * 24 * 60 * 60 * 1000;

    @Override
    protected Long genValue() {
        return Math.abs(random.nextLong()) % tenYears + base;
    }
}
