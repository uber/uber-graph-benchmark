package com.uber.ugb.model;

import com.uber.ugb.model.generator.Generator;
import com.uber.ugb.model.generator.GeneratorFactory;
import com.uber.ugb.model.generator.WeightedValueslGenerator;
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
                         Map<String, StatisticsSpec.PropertyValueWeight[]> propertyModels) {
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
