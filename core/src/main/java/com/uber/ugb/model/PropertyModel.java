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

package com.uber.ugb.model;

import com.uber.ugb.model.generator.Generator;
import com.uber.ugb.model.generator.GeneratorFactory;
import com.uber.ugb.model.generator.WeightedValueslGenerator;
import com.uber.ugb.schema.QualifiedName;
import com.uber.ugb.schema.Vocabulary;
import com.uber.ugb.schema.model.EntityType;
import com.uber.ugb.schema.model.RelationType;
import com.uber.ugb.schema.model.Type;
import com.uber.ugb.statistics.StatisticsSpec;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public class PropertyModel implements Serializable {
    private static final long serialVersionUID = -8771790313378833590L;

    private final Collection<SimpleProperty> properties = new LinkedList<>();

    private static final GeneratorFactory generatorFactory = new GeneratorFactory();

    public PropertyModel(Vocabulary vocabulary, Type entityType,
                         Map<QualifiedName, StatisticsSpec.PropertyValueWeight[]> propertyModels) {
        Set<RelationType> propertiesAndEdges = vocabulary.getConcreteRelationTypesFrom(entityType);
        for(RelationType relationType: propertiesAndEdges){
            if (!(relationType.getTo() instanceof EntityType)){
                continue;
            }
            EntityType toType = (EntityType) relationType.getTo();
            if (!toType.getIsDataType()){
                continue;
            }
            if (propertyModels.containsKey(relationType.getName().toString())){
                StatisticsSpec.PropertyValueWeight[] vw = propertyModels.get(relationType.getName().toString());
                properties.add(new SimpleProperty(relationType, new WeightedValueslGenerator(vw)));
            }else{
                Generator generator = generatorFactory.make(vocabulary, toType);
                properties.add(new SimpleProperty(relationType, generator));
            }
        }
    }

    public Collection<SimpleProperty> getProperties() {
        return properties;
    }

}
