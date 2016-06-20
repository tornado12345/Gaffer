/*
 * Copyright 2016 Crown Copyright
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
package gaffer.accumulostore.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.integration.AbstractStoreIT;
import gaffer.integration.TraitRequirement;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.store.StoreProperties;
import gaffer.store.StoreTrait;
import gaffer.store.schema.Schema;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

public class AccumuloAggregationIT extends AbstractStoreIT {
    private static final StoreProperties STORE_PROPERTIES = StoreProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreITs.class));
    private static final Schema STORE_SCHEMA = Schema.fromJson(StreamUtil.openStream(AccumuloStoreITs.class, "/schema-IT/accumuloAggregationstoreTypes.json"));

    private final String AGGREGATED_SOURCE = SOURCE + 6;
    private final String AGGREGATED_DEST = DEST + 6;

    private final Entity SIMILAR_ENTITY = new Entity.Builder()
            .vertex(AGGREGATED_SOURCE)
            .group(TestGroups.ENTITY)
            .property(TestPropertyNames.STRING, "3b") // different column family property
            .build();

    private final Edge SIMILAR_EDGE = new Edge.Builder()
            .source(AGGREGATED_SOURCE).dest(AGGREGATED_DEST).directed(false)
            .group(TestGroups.EDGE)
            .property(TestPropertyNames.INT, 100) // different column qualifier property
            .property(TestPropertyNames.COUNT, 1L)
            .build();


    @Before
    public void setup() throws Exception {
        setStoreSchema(STORE_SCHEMA);
        setStoreProperties(STORE_PROPERTIES);
        super.setup();

        addDefaultElements();

        // Add similar elements
        graph.execute(new AddElements.Builder()
                .elements(Arrays.asList(SIMILAR_ENTITY, SIMILAR_EDGE))
                .build(), getUser());
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void shouldNotAggregateColumnFamilyOrQualifierWithSummariseFlagOff() throws OperationException, UnsupportedEncodingException {
        // Given
        final GetRelatedElements<ElementSeed, Element> getElements = new GetRelatedElements.Builder<>()
                .addSeed(new EntitySeed(AGGREGATED_SOURCE))
                .summarise(false)
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, getUser()));

        // Then
        assertNotNull(results);
        assertEquals(4, results.size());

        final Entity expectedEntity = new Entity(TestGroups.ENTITY, AGGREGATED_SOURCE);
        expectedEntity.putProperty(TestPropertyNames.STRING, "3");

        final Edge expectedEdge = new Edge(TestGroups.EDGE, AGGREGATED_SOURCE, AGGREGATED_DEST, false);
        expectedEdge.putProperty(TestPropertyNames.INT, 1);
        expectedEdge.putProperty(TestPropertyNames.COUNT, 1L);

        assertThat(results, IsCollectionContaining.hasItems(
                getEntity(AGGREGATED_SOURCE),
                getEdge(AGGREGATED_SOURCE, AGGREGATED_DEST, false),
                SIMILAR_ENTITY,
                SIMILAR_EDGE
        ));
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void shouldAggregateColumnQualifierButNotFamilyWithSummariseFlagOn() throws OperationException, UnsupportedEncodingException {
        // Given
        final GetRelatedElements<ElementSeed, Element> getElements = new GetRelatedElements.Builder<>()
                .addSeed(new EntitySeed(AGGREGATED_SOURCE))
                .summarise(true)
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, getUser()));

        // Then
        assertNotNull(results);
        assertEquals(3, results.size());

        final Edge expectedEdge = new Edge(TestGroups.EDGE, AGGREGATED_SOURCE, AGGREGATED_DEST, false);
        expectedEdge.putProperty(TestPropertyNames.INT, 100);
        expectedEdge.putProperty(TestPropertyNames.COUNT, 2L);

        assertThat(results, IsCollectionContaining.hasItems(
                expectedEdge,
                getEntity(AGGREGATED_SOURCE),
                SIMILAR_ENTITY
        ));
    }
}
