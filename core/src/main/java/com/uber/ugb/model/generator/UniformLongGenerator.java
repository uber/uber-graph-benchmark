package com.uber.ugb.model.generator;

public class UniformLongGenerator extends Generator<Long> {

    private final long lowerBound, upperBound, range;

    public UniformLongGenerator(long lowerBound, long upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.range = this.upperBound - this.lowerBound + 1;
    }

    @Override
    protected Long genValue() {
        return Math.abs(random.nextLong()) % this.range + lowerBound;
    }

}
