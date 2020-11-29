/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.Seq;
import scala.collection.mutable.WrappedArray$;

import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.ExtractKeyFromRow;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.fail;

public class ExtractKeyFromRowTest {
    private LinkedHashSet<String> groupByColumns;
    private Map<String, String[]> columnsToPaths;
    private SchemaUtils utils;

    @BeforeEach
    public void setUp() {
        groupByColumns = new LinkedHashSet<>();
        groupByColumns.add("double");
        groupByColumns.add("date");
        groupByColumns.add("treeSet");
        columnsToPaths = new HashMap<>();
        String[] treeSetPropertyPaths = new String[1];
        treeSetPropertyPaths[0] = "treeSet.list.element";
        String[] doublePropertyPaths = new String[1];
        doublePropertyPaths[0] = "double";
        String[] datePropertyPaths = new String[1];
        datePropertyPaths[0] = "date";
        String[] vertPaths = new String[1];
        vertPaths[0] = ParquetStore.VERTEX;
        String[] srcPaths = new String[1];
        srcPaths[0] = ParquetStore.SOURCE;
        String[] dstPaths = new String[1];
        dstPaths[0] = ParquetStore.DESTINATION;
        columnsToPaths.put("treeSet", treeSetPropertyPaths);
        columnsToPaths.put("double", doublePropertyPaths);
        columnsToPaths.put("date", datePropertyPaths);
        columnsToPaths.put(ParquetStore.VERTEX, vertPaths);
        columnsToPaths.put(ParquetStore.SOURCE, srcPaths);
        columnsToPaths.put(ParquetStore.DESTINATION, dstPaths);
        final Schema schema = TestUtils.gafferSchema("schemaUsingStringVertexType");
        utils = new SchemaUtils(schema);
    }

    @Test
    public void testExtractKeyFromRowForEntity() throws Exception {
        final ExtractKeyFromRow entityConverter = new ExtractKeyFromRow(groupByColumns, columnsToPaths, true, false);
        final Row row = DataGen.generateEntityRow(utils, "BasicEntity", "vertex", (byte) 'a', 0.2,
                3f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), null);
        final Seq<Object> results = entityConverter.call(row);
        final List<Object> actual = new ArrayList<>();
        for (int i = 0; i < results.length(); i++) {
            actual.add(results.apply(i));
        }
        final List<Object> expected = new ArrayList<>();
        expected.add(0.2);
        expected.add("vertex");
        expected.add(TestUtils.DATE.getTime());
        expected.add(WrappedArray$.MODULE$.make(TestUtils.getTreeSet1().toArray()));
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void testExtractKeyFromRowForEdge() throws Exception {
        final ExtractKeyFromRow edgeConverter = new ExtractKeyFromRow(groupByColumns, columnsToPaths, false, false);
        final Row row = DataGen.generateEdgeRow(utils, "BasicEdge", "src", "dst", true,
                (byte) 'a', 0.2, 3f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE,
                TestUtils.getFreqMap1(), null);
        final Seq<Object> results = edgeConverter.call(row);
        final List<Object> actual = new ArrayList<>(6);
        for (int i = 0; i < results.length(); i++) {
            actual.add(results.apply(i));
        }
        final List<Object> expected = new ArrayList<>(6);
        expected.add(WrappedArray$.MODULE$.make(TestUtils.getTreeSet1().toArray()));
        expected.add(0.2);
        expected.add("dst");
        expected.add("src");
        expected.add(true);
        expected.add(TestUtils.DATE.getTime());
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void testExtractKeyFromEmptyRow() {
        final ExtractKeyFromRow edgeConverter = new ExtractKeyFromRow(groupByColumns, columnsToPaths, false, false);
        try {
            edgeConverter.call(Row$.MODULE$.empty());
            fail();
        } catch (final Exception ignored) {
        }
    }
}
