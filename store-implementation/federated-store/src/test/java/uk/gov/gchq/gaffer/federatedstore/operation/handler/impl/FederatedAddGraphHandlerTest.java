/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.authUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.testUser;

public class FederatedAddGraphHandlerTest {
    private static final String FEDERATEDSTORE_GRAPH_ID = "federatedStore";
    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private static final String EXPECTED_GRAPH_ID_2 = "testGraphID2";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final String EXCEPTION_EXPECTED = "Exception expected";
    private static final AccumuloProperties storeProperties = new AccumuloProperties();
    private User testUser;
    private User authUser;
    private FederatedStore store;
    private FederatedStoreProperties federatedStoreProperties;


    @Before
    public void setUp() throws Exception {
        CacheServiceLoader.shutdown();
        this.store = new FederatedStore();
        federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        storeProperties.setStoreClass(SingleUseMockAccumuloStore.class);

        testUser = testUser();
        authUser = authUser();
    }

    @Test
    public void shouldAddGraph() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);
        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(testUser, null);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(EXPECTED_GRAPH_ID, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID_2)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(testUser),
                store);

        graphs = store.getGraphs(testUser, null);

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        final HashSet<String> set = Sets.newHashSet();
        while (iterator.hasNext()) {
            set.add(iterator.next().getGraphId());
        }
        assertTrue(set.contains(EXPECTED_GRAPH_ID));
        assertTrue(set.contains(EXPECTED_GRAPH_ID_2));
    }

    @Test
    public void shouldAddDisabledByDefaultGraph() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);
        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .disabledByDefault(true)
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> enabledGraphs = store.getGraphs(testUser, null);
        assertEquals(0, enabledGraphs.size());


        Collection<Graph> expectedGraphs = store.getGraphs(testUser, EXPECTED_GRAPH_ID);
        assertEquals(1, expectedGraphs.size());
        assertEquals(EXPECTED_GRAPH_ID, expectedGraphs.iterator().next().getGraphId());
    }

    @Test
    public void shouldAddGraphUsingLibrary() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        Schema expectedSchema = new Schema.Builder().build();

        storeProperties.setStorePropertiesClass(AccumuloProperties.class);

        assertEquals(0, store.getGraphs(testUser, null).size());
        assertEquals(0, store.getGraphs(testUser, null).size());


        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(testUser, null);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(EXPECTED_GRAPH_ID, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        final GraphLibrary library = new HashMapGraphLibrary();
        library.add(EXPECTED_GRAPH_ID_2, expectedSchema, storeProperties);
        store.setGraphLibrary(library);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID_2)
                        .build(),
                new Context(testUser),
                store);

        graphs = store.getGraphs(testUser, null);

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        final HashSet<String> set = Sets.newHashSet();
        while (iterator.hasNext()) {
            set.add(iterator.next().getGraphId());
        }

        assertTrue(set.contains(EXPECTED_GRAPH_ID));
        assertTrue(set.contains(EXPECTED_GRAPH_ID_2));
    }

    @Test
    public void shouldThrowWhenOverwriteGraphIsDifferent() throws Exception {
        Schema expectedSchema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .build())
                .type("string", String.class)
                .build();

        assertEquals(0, store.getGraphs(testUser, null).size());

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), federatedStoreProperties);

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(testUser),
                store);

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(EXPECTED_GRAPH_ID)
                            .schema(expectedSchema)
                            .schema(new Schema.Builder()
                                    .type("unusual", String.class)
                                    .build())
                            .storeProperties(storeProperties)
                            .build(),
                    new Context(testUser),
                    store);
            fail(EXCEPTION_EXPECTED);
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, EXPECTED_GRAPH_ID)));
        }
    }

    @Test
    public void shouldThrowWhenOverwriteGraphIsSameAndAccessIsDifferent() throws Exception {
        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null).size());

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), federatedStoreProperties);

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(testUser),
                store);

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(EXPECTED_GRAPH_ID)
                            .schema(expectedSchema)
                            .graphAuths("X")
                            .storeProperties(storeProperties)
                            .build(),
                    new Context(testUser),
                    store);
            fail(EXCEPTION_EXPECTED);
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, EXPECTED_GRAPH_ID)));
        }
    }

    @Test
    public void shouldNotThrowWhenOverwriteGraphIsSame() throws Exception {
        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null).size());

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), federatedStoreProperties);

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(testUser),
                store);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(testUser),
                store);
    }

    @Test
    public void shouldAddGraphIDOnlyWithAuths() throws Exception {

        federatedStoreProperties.setCustomPropertyAuths("auth1,auth2");
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(EXPECTED_GRAPH_ID)
                            .schema(expectedSchema)
                            .storeProperties(storeProperties)
                            .build(),
                    new Context(testUser),
                    store);
            fail(EXCEPTION_EXPECTED);
        } catch (OperationException e) {
            assertEquals(String.format(FederatedAddGraphHandler.USER_IS_LIMITED_TO_ONLY_USING_PARENT_PROPERTIES_ID_FROM_GRAPHLIBRARY_BUT_FOUND_STORE_PROPERTIES_S, "{gaffer.store.class=uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore, gaffer.store.properties.class=uk.gov.gchq.gaffer.accumulostore.AccumuloProperties}"), e.getMessage());
        }

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(authUser),
                store);

        final Collection<Graph> graphs = store.getGraphs(authUser, null);
        assertEquals(1, graphs.size());
        assertEquals(0, store.getGraphs(testUser, null).size());
        assertEquals(EXPECTED_GRAPH_ID, graphs.iterator().next().getGraphId());
    }

    /**
     * Replicating a bug condition when setting auths the
     * FederatedAddGraphHandler didn't set the adding user.
     *
     * @throws Exception
     */
    @Test
    public void shouldAddGraphWithAuthsAndAddingUser() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null).size());

        AccumuloProperties storeProperties = new AccumuloProperties();
        storeProperties.setStorePropertiesClass(AccumuloProperties.class);
        storeProperties.setStoreClass(SingleUseMockAccumuloStore.class);

        new FederatedAddGraphHandler().doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .graphAuths("testAuth")
                        .build(),
                new Context(testUser),
                store);

        final CloseableIterable<? extends Element> elements = new FederatedGetAllElementsHandler().doOperation(
                new GetAllElements(),
                new Context(testUser),
                store);

        assertNotNull(elements);
    }
}
