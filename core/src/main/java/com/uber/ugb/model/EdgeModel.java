package com.uber.ugb.model;

import java.io.Serializable;

public class EdgeModel implements Serializable {
    private static final long serialVersionUID = 5511473326817973109L;

    private final Incidence domainIncidence;
    private final Incidence randeIncidence;

    public EdgeModel(final Incidence domainIncidence,
              final Incidence randeIncidence) {
        this.domainIncidence = domainIncidence;
        this.randeIncidence = randeIncidence;
    }

    public Incidence getDomainIncidence() {
        return domainIncidence;
    }

    public Incidence getRangeIncidence() {
        return randeIncidence;
    }
}
