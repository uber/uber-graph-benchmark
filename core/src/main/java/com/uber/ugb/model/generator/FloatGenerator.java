package com.uber.ugb.model.generator;

public class FloatGenerator extends Generator<Float> {

    float min;
    float max;
    float range;

    public FloatGenerator(float min, float max) {
        this.min = min;
        this.max = max;
        this.range = max - min;
    }

    @Override
    protected Float genValue() {
        return Math.abs(random.nextFloat() * this.range) + this.min;
    }
}
