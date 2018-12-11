/*
 *
 *  * Copyright 2018 Uber Technologies Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.uber.ugb.model.distro;

import com.google.common.base.Preconditions;

import java.util.Random;

public class ConstantDegreeDistribution implements DegreeDistribution {
    private static final long serialVersionUID = 5619558013797593115L;

    private final int degree;

    public ConstantDegreeDistribution(int degree) {
        // other constant degrees are not yet needed
        Preconditions.checkArgument(degree == 1);

        this.degree = degree;
    }

    @Override
    public Sample createSample(final int size, final Random random) {
        return new Sample() {
            private int nextIndex = 0;

            @Override
            public int getNextDegree() {
                return degree;
            }

            @Override
            public int getNextIndex() {
                return nextIndex++;
            }
        };
    }
}
