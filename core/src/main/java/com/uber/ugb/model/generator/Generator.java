package com.uber.ugb.model.generator;

import java.util.Random;

public abstract class Generator<V> {

    protected Random random = new Random();

    protected abstract V genValue();

    public Object generate(String label, long id, String key) {
        long seed = (id << 5) - id;
        seed = ((seed << 5) - seed) + label.hashCode();
        seed = ((seed << 5) - seed) + ".".hashCode();
        seed = ((seed << 5) - seed) + key.hashCode();
        this.random.setSeed(seed);
        return genValue();
    }

}
