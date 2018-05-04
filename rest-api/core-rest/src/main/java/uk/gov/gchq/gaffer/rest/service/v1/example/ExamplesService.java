/*
 * Copyright 2016-2018 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.rest.service.v1.example;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.rest.example.ExampleDomainObject;
import uk.gov.gchq.gaffer.rest.example.ExampleDomainObjectGenerator;
import uk.gov.gchq.gaffer.rest.example.ExampleElementGenerator;
import uk.gov.gchq.gaffer.rest.example.ExampleFilterFunction;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

/**
 * Implementation of the {@link IExamplesService}.
 */
public class ExamplesService implements IExamplesService {
    public static final String TRANSFORMED_PROPERTIES = "transformedProperties";

    @Inject
    private GraphFactory graphFactory;

    @Inject
    private UserFactory userFactory;

    @Override
    public OperationChain execute() {
        return new OperationChain.Builder()
                .first(getAllElements())
                .then(new Limit.Builder<Element>()
                        .resultLimit(1)
                        .build())
                .build();
    }

    @Override
    public Operation executeOperation() {
        return getAllElements();
    }

    @Override
    public OperationChain executeChunked() {
        return execute();
    }

    @Override
    public Operation executeChunkedOperation() {
        return executeOperation();
    }

    @Override
    public GetElements getElementsBySeed() {
        final GetElements op = new GetElements();
        final List<ElementId> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntityId(1));
        }

        if (hasEdges()) {
            seeds.add(getEdgeId(1, 2));
        }

        op.setInput(seeds);
        populateOperation(op);
        return op;
    }

    @Override
    public GetElements getRelatedElements() {
        final GetElements op = new GetElements();
        final List<ElementId> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntityId(1));
        } else if (hasEdges()) {
            seeds.add(new EntitySeed(getEdgeId(1, 2).getSource()));
        }

        if (hasEdges()) {
            seeds.add(getEdgeId(1, 2));
        }

        op.setInput(seeds);
        populateOperation(op);
        return op;
    }

    @Override
    public GetAdjacentIds getAdjacentIds() {
        final GetAdjacentIds op = new GetAdjacentIds();
        final List<EntityId> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntityId(1));
        } else if (hasEdges()) {
            seeds.add(new EntitySeed(getEdgeId(1, 2).getSource()));
        }

        op.setInput(seeds);
        populateOperation(op);
        return op;
    }

    @Override
    public GetAllElements getAllElements() {
        final GetAllElements op = new GetAllElements();
        populateOperation(op);
        return op;
    }

    @Override
    public GetElements getElements() {
        final GetElements op = new GetElements();
        final List<ElementId> seeds = new ArrayList<>();
        if (hasEntities()) {
            seeds.add(getEntityId(1));
        } else if (hasEdges()) {
            seeds.add(new EntitySeed(getEdgeId(1, 2).getSource()));
        }

        if (hasEdges()) {
            seeds.add(getEdgeId(1, 2));
        }

        op.setInput(seeds);
        populateOperation(op);
        return op;
    }


    @Override
    public AddElements addElements() {
        final AddElements op = new AddElements();
        List<Element> elements = new ArrayList<>();
        if (hasEntities()) {
            elements.add(getEntity(1));
            elements.add(getEntity(2));
        }
        if (hasEdges()) {
            elements.add(getEdge(1, 2));
        }

        op.setInput(elements);

        populateOperation(op);
        return op;
    }

    @Override
    public GenerateObjects generateObjects() {
        final GenerateObjects<ExampleDomainObject> op = new GenerateObjects<>(new ExampleDomainObjectGenerator());
        List<Element> elements = new ArrayList<>();
        if (hasEntities()) {
            elements.add(getEntity(1));
            elements.add(getEntity(2));
        }
        if (hasEdges()) {
            elements.add(getEdge(1, 2));
        }
        op.setInput(elements);
        populateOperation(op);
        return op;
    }

    @Override
    public GenerateElements generateElements() {
        final GenerateElements<ExampleDomainObject> op = new GenerateElements<>(new ExampleElementGenerator());
        final ArrayList<ExampleDomainObject> objs = new ArrayList<>();
        if (hasEntities()) {
            final SchemaElementDefinition entityDef = getSchema().getEntity(getAnEntityGroup());
            objs.add(new ExampleDomainObject(getAnEntityGroup(),
                    getExampleVertex(entityDef.getIdentifierClass(IdentifierType.VERTEX), 1)));
            objs.add(new ExampleDomainObject(getAnEntityGroup(),
                    getExampleVertex(entityDef.getIdentifierClass(IdentifierType.VERTEX), 2)));
        }


        if (hasEdges()) {
            final SchemaElementDefinition edgeDef = getSchema().getEdge(getAnEdgeGroup());
            objs.add(new ExampleDomainObject(getAnEdgeGroup(),
                    getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.SOURCE), 1),
                    getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.DESTINATION), 1),
                    isAnEdgeDirected()));
        }

        op.setInput(objs);
        populateOperation(op);
        return op;
    }

    private Schema getSchema() {
        return graphFactory.getGraph().getSchema();
    }

    private void populateOperation(final Output operation) {
        populateOperation((Operation) operation);
    }

    public View.Builder generateViewBuilder() {
        final View.Builder viewBuilder = new View.Builder();
        if (hasEntities()) {
            final ViewElementDefinition viewElement;
            if (null == getAnEntityPropertyName()) {
                viewElement = new ViewElementDefinition();
            } else {
                viewElement = new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(getAnEntityPropertyName())
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build();
            }
            viewBuilder.entity(getAnEntityGroup(), viewElement);
        }

        if (hasEdges()) {
            final ViewElementDefinition viewElement;
            if (null == getAnEdgePropertyName()) {
                viewElement = new ViewElementDefinition();
            } else {
                viewElement = new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(getAnEdgePropertyName())
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build();
            }
            viewBuilder.edge(getAnEdgeGroup(), viewElement);
        }

        viewBuilder.globalElements(new GlobalViewElementDefinition.Builder()
                .groupBy()
                .build());

        return viewBuilder;
    }

    protected void populateOperation(final Operation operation) {
        // override to add options to the operation
    }

    protected Entity getEntity(final int uniqueId) {
        final String group = getAnEntityGroup();
        final SchemaElementDefinition entityDef = getSchema().getEntity(group);

        final Entity entity = new Entity.Builder()
                .group(group)
                .vertex(getExampleVertex(entityDef
                        .getIdentifierClass(IdentifierType.VERTEX), uniqueId))
                .build();
        populateProperties(entity, entityDef, uniqueId);

        return entity;
    }

    protected Edge getEdge(final int uniqueId1, final int uniqueId2) {
        final String group = getAnEdgeGroup();
        final SchemaElementDefinition edgeDef = getSchema().getEdge(group);

        final Edge edge = new Edge.Builder()
                .group(group)
                .source(getExampleVertex(edgeDef.getIdentifierClass(IdentifierType.SOURCE), uniqueId1))
                .dest(getExampleVertex(edgeDef
                        .getIdentifierClass(IdentifierType.DESTINATION), uniqueId2))
                .directed(isAnEdgeDirected())
                .build();

        populateProperties(edge, edgeDef, uniqueId1);

        return edge;
    }

    protected EntityId getEntityId(final int uniqueId) {
        return new EntitySeed(
                getExampleVertex(getSchema().getEntity(getAnEntityGroup()).getIdentifierClass(IdentifierType.VERTEX), uniqueId));
    }

    protected EdgeId getEdgeId(final int uniqueId1, final int uniqueId2) {
        return new EdgeSeed(
                getExampleVertex(getSchema().getEdge(getAnEdgeGroup()).getIdentifierClass(IdentifierType.SOURCE), uniqueId1),
                getExampleVertex(getSchema().getEdge(getAnEdgeGroup()).getIdentifierClass(IdentifierType.DESTINATION), uniqueId2),
                isAnEdgeDirected());
    }

    protected boolean isAnEdgeDirected() {
        return !getSchema().getEdge(getAnEdgeGroup()).getDirected().toLowerCase(Locale.getDefault()).contains("false");
    }

    protected String getAnEntityPropertyName() {
        final SchemaElementDefinition entityDef = getSchema().getEntity(getAnEntityGroup());
        String propertyName = null;
        if (null != entityDef && !entityDef.getProperties().isEmpty()) {
            propertyName = entityDef.getProperties().iterator().next();
        }

        return propertyName;
    }

    protected String getAnEntityGroup() {
        if (!getSchema().getEntityGroups().isEmpty()) {
            for (final Entry<String, SchemaEntityDefinition> entry : getSchema().getEntities().entrySet()) {
                // Try and find an entity that has properties
                if (null != entry.getValue().getProperties() && !entry.getValue().getProperties().isEmpty()) {
                    return entry.getKey();
                }
            }
            // if no entities have properties just return the first entity.
            return getSchema().getEntityGroups().iterator().next();
        } else {
            return "exampleEntityGroup";
        }
    }

    protected String getAnEdgePropertyName() {
        final SchemaElementDefinition edgeDef = getSchema().getEdge(getAnEdgeGroup());
        final String propertyName;
        if (null != edgeDef && !edgeDef.getProperties().isEmpty()) {
            propertyName = edgeDef.getProperties().iterator().next();
        } else {
            propertyName = "examplePropertyName";
        }

        return propertyName;
    }

    protected String getAnEdgeGroup() {
        if (!getSchema().getEdgeGroups().isEmpty()) {
            for (final Entry<String, SchemaEdgeDefinition> entry : getSchema().getEdges().entrySet()) {
                // Try and find an edge that has properties
                if (null != entry.getValue().getProperties() && !entry.getValue().getProperties().isEmpty()) {
                    return entry.getKey();
                }
            }
            // if no edges have properties just return the first entity.
            return getSchema().getEdgeGroups().iterator().next();
        } else {
            return "exampleEdgeGroup";
        }
    }

    protected boolean hasEdges() {
        return !getSchema().getEdges().isEmpty();
    }

    protected boolean hasEntities() {
        return !getSchema().getEntities().isEmpty();
    }

    protected void populateProperties(final Element element, final SchemaElementDefinition elementDef, final int uniqueId) {
        for (final String property : elementDef.getProperties()) {
            element.putProperty(property, getExampleValue(elementDef.getPropertyClass(property), uniqueId));
        }
    }

    protected Object getExampleVertex(final Class<?> clazz, final int uniqueId) {
        if (String.class.equals(clazz) || Object.class.equals(clazz)) {
            return "vertex" + uniqueId;
        }

        return getExampleValue(clazz, uniqueId);
    }

    protected Object getExampleValue(final Class clazz, final int uniqueId) {
        Object value;
        if (null == clazz) {
            value = null;
        } else if (String.class.equals(clazz) || Object.class.equals(clazz)) {
            value = String.valueOf(uniqueId);
        } else if (Integer.class.equals(clazz)) {
            value = uniqueId;
        } else if (Double.class.equals(clazz)) {
            value = (double) uniqueId + 0.1;
        } else if (Long.class.equals(clazz)) {
            value = (long) uniqueId;
        } else if (Float.class.equals(clazz)) {
            value = (float) uniqueId;
        } else if (Date.class.equals(clazz)) {
            value = new Date(System.currentTimeMillis() - 10000 + uniqueId);
        } else {
            try {
                if (clazz.isEnum()) {
                    List l = Arrays.asList(clazz.getEnumConstants());
                    if (!l.isEmpty()) {
                        value = Enum.valueOf(clazz, l.get(0).toString());
                    } else {
                        value = clazz.newInstance();
                    }
                } else {
                    value = clazz.newInstance();
                }
            } catch (final InstantiationException | IllegalAccessException e) {
                value = null;
            }
        }

        return value;
    }
}
