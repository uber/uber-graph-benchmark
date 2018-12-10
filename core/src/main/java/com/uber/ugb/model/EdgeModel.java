package com.uber.ugb.model;

import java.io.Serializable;

public class EdgeModel implements Serializable {
    private static final long serialVersionUID = 5511473326817973109L;

    private final Incidence domainIncidence;
    private final Incidence rangeIncidence;

    public EdgeModel(final Incidence domainIncidence,
              final Incidence rangeIncidence) {
        this.domainIncidence = domainIncidence;
        this.rangeIncidence = rangeIncidence;
    }

    public Incidence getDomainIncidence() {
        return domainIncidence;
    }

    public Incidence getRangeIncidence() {
        return rangeIncidence;
    }
}
