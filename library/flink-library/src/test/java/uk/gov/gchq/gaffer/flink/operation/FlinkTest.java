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

package uk.gov.gchq.gaffer.flink.operation;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

public abstract class FlinkTest {
    public static final Schema SCHEMA = new Schema.Builder()
            .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .build())
            .type(TestTypes.PROP_COUNT, new TypeDefinition.Builder()
                    .clazz(Long.class)
                    .aggregateFunction(new Sum())
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex(TestTypes.ID_STRING)
                    .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                    .build())
            .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                    .vertex(TestTypes.ID_STRING)
                    .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                    .aggregate(false)
                    .build())
            .build();

    public static final java.util.List<? extends Element> EXPECTED_ELEMENTS = Lists.newArrayList(
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("1")
                    .property(TestPropertyNames.COUNT, 3L)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("2")
                    .property(TestPropertyNames.COUNT, 2L)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("3")
                    .property(TestPropertyNames.COUNT, 1L)
                    .build(),

            new Entity.Builder()
                    .group(TestGroups.ENTITY_2)
                    .vertex("1")
                    .property(TestPropertyNames.COUNT, 1L)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY_2)
                    .vertex("1")
                    .property(TestPropertyNames.COUNT, 1L)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY_2)
                    .vertex("1")
                    .property(TestPropertyNames.COUNT, 1L)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY_2)
                    .vertex("2")
                    .property(TestPropertyNames.COUNT, 1L)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY_2)
                    .vertex("2")
                    .property(TestPropertyNames.COUNT, 1L)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY_2)
                    .vertex("3")
                    .property(TestPropertyNames.COUNT, 1L)
                    .build()
    );

    public static final String[] DATA_VALUES = {"1", "1", "2", "3", "1", "2"};
    public static final String DATA = StringUtils.join(DATA_VALUES, "\n");
    public static final byte[] DATA_BYTES = StringUtil.toBytes(DATA);

    public static Graph createGraph() {
        return new Graph.Builder()
                .store(createStore())
                .build();
    }

    public static Store createStore() {
        return Store.createStore("graphId", SCHEMA, MapStoreProperties.loadStoreProperties("store.properties"));
    }

    public static void verifyElements(final Graph graph) throws OperationException, InterruptedException {
        // Wait for the elements to be ingested.
        Thread.sleep(2000);
        final Iterable<? extends Element> allElements = graph.execute(new GetAllElements(), new User());
        ElementUtil.assertElementEquals(EXPECTED_ELEMENTS, allElements);
    }
}
