package com.uber.ugb.model.generator;

public class BooleanGenerator extends Generator<Boolean> {

    @Override
    protected Boolean genValue() {
        return random.nextBoolean();
    }
}
