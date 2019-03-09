/*
 * Copyright 2016-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;
import scala.collection.mutable.Map;
import scala.collection.mutable.Map$;
import scala.collection.mutable.MutableList;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.exception.ConversionException;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.Converter;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * These tests test that the handler for {@link GetDataFrameOfElements} operate correctly. Note however that
 * Spark will filter out results that don't match the supplied predicates itself. The unit tests of
 * {@link AccumuloStoreRelation} ensure that the RDD that is returned has already had the correct filtering
 * applied in Accumulo.
 */
public class GetDataFrameOfElementsHandlerTest {

    static final String ENTITY_GROUP = "BasicEntity";
    static final String EDGE_GROUP = "BasicEdge";
    static final String EDGE_GROUP2 = "BasicEdge2";
    private static final int NUM_ELEMENTS = 10;

    @Test
    public void checkGetCorrectElementsInDataFrame() throws OperationException {
        final Graph graph = getGraph("/schema-DataFrame/elements.json", getElements());

        // Edges group - check get correct edges
        GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().edge(EDGE_GROUP).build())
                .build();
        Dataset<Row> dataFrame = graph.execute(dfOperation, new User());
        Set<Row> results = new HashSet<>(dataFrame.collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final MutableList<Object> fields1 = new MutableList<>();
            fields1.appendElem(EDGE_GROUP);
            fields1.appendElem("" + i);
            fields1.appendElem("B");
            fields1.appendElem(true);
            fields1.appendElem(null);
            fields1.appendElem(1);
            fields1.appendElem(2);
            fields1.appendElem(3.0F);
            fields1.appendElem(4.0D);
            fields1.appendElem(5L);
            fields1.appendElem(100L);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
            final MutableList<Object> fields2 = new MutableList<>();
            fields2.appendElem(EDGE_GROUP);
            fields2.appendElem("" + i);
            fields2.appendElem("C");
            fields2.appendElem(true);
            fields2.appendElem(null);
            fields2.appendElem(6);
            fields2.appendElem(7);
            fields2.appendElem(8.0F);
            fields2.appendElem(9.0D);
            fields2.appendElem(10L);
            fields2.appendElem(i * 200L);
            expectedRows.add(Row$.MODULE$.fromSeq(fields2));
        }
        assertEquals(expectedRows, results);

        // Entities group - check get correct entities
        dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().entity(ENTITY_GROUP).build())
                .build();
        dataFrame = graph.execute(dfOperation, new User());
        results.clear();
        results.addAll(dataFrame.collectAsList());
        expectedRows.clear();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final MutableList<Object> fields1 = new MutableList<>();
            fields1.clear();
            fields1.appendElem(ENTITY_GROUP);
            fields1.appendElem("" + i);
            fields1.appendElem(1);
            fields1.appendElem(i);
            fields1.appendElem(3.0F);
            fields1.appendElem(4.0D);
            fields1.appendElem(5L);
            fields1.appendElem(6);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
        }
        assertEquals(expectedRows, results);
    }

    @Test
    public void checkGetCorrectElementsInDataFrameMultipleGroups() throws OperationException {
        final Graph graph = getGraph("/schema-DataFrame/elements.json", getElements());

        // Use entity and edges group - check get correct data
        GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().entity(ENTITY_GROUP).edge(EDGE_GROUP).build())
                .build();
        Dataset<Row> dataFrame = graph.execute(dfOperation, new User());
        final Set<Row> results = new HashSet<>(dataFrame.collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final MutableList<Object> fields1 = new MutableList<>();
            fields1.appendElem(EDGE_GROUP);
            fields1.appendElem(null);
            fields1.appendElem(1);
            fields1.appendElem(2);
            fields1.appendElem(3.0F);
            fields1.appendElem(4.0D);
            fields1.appendElem(5L);
            fields1.appendElem(100L);
            fields1.appendElem("" + i);
            fields1.appendElem("B");
            fields1.appendElem(true);
            fields1.appendElem(null);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
            final MutableList<Object> fields2 = new MutableList<>();
            fields2.appendElem(EDGE_GROUP);
            fields2.appendElem(null);
            fields2.appendElem(6);
            fields2.appendElem(7);
            fields2.appendElem(8.0F);
            fields2.appendElem(9.0D);
            fields2.appendElem(10L);
            fields2.appendElem(i * 200L);
            fields2.appendElem("" + i);
            fields2.appendElem("C");
            fields2.appendElem(true);
            fields2.appendElem(null);
            expectedRows.add(Row$.MODULE$.fromSeq(fields2));
            final MutableList<Object> fields3 = new MutableList<>();
            fields3.appendElem(ENTITY_GROUP);
            fields3.appendElem("" + i);
            fields3.appendElem(1);
            fields3.appendElem(i);
            fields3.appendElem(3.0F);
            fields3.appendElem(4.0D);
            fields3.appendElem(5L);
            fields3.appendElem(6);
            fields3.appendElem(null);
            fields3.appendElem(null);
            fields3.appendElem(null);
            fields3.appendElem(null);
            expectedRows.add(Row$.MODULE$.fromSeq(fields3));
        }
        assertEquals(expectedRows, results);

        // Entities group - check get correct entities
        dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().entity(ENTITY_GROUP).build())
                .build();
        dataFrame = graph.execute(dfOperation, new User());
        results.clear();
        results.addAll(dataFrame.collectAsList());
        expectedRows.clear();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final MutableList<Object> fields1 = new MutableList<>();
            fields1.clear();
            fields1.appendElem(ENTITY_GROUP);
            fields1.appendElem("" + i);
            fields1.appendElem(1);
            fields1.appendElem(i);
            fields1.appendElem(3.0F);
            fields1.appendElem(4.0D);
            fields1.appendElem(5L);
            fields1.appendElem(6);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
        }
        assertEquals(expectedRows, results);
    }

    @Test
    public void checkGetCorrectElementsInDataFrameWithProjection() throws OperationException {
        final Graph graph = getGraph("/schema-DataFrame/elements.json", getElements());

        // Get all edges
        final GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().edge(EDGE_GROUP).build())
                .build();
        final Dataset<Row> dataFrame = graph.execute(dfOperation, new User());

        // Check get correct rows when ask for src, dst and property2 columns
        Set<Row> results = new HashSet<>(dataFrame.select("src", "dst", "property2").collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            MutableList<Object> fields1 = new MutableList<>();
            fields1.appendElem("" + i);
            fields1.appendElem("B");
            fields1.appendElem(3.0F);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
            MutableList<Object> fields2 = new MutableList<>();
            fields2.appendElem("" + i);
            fields2.appendElem("C");
            fields2.appendElem(8.0F);
            expectedRows.add(Row$.MODULE$.fromSeq(fields2));
        }
        assertEquals(expectedRows, results);

        // Check get correct rows when ask for just property2 column
        results = new HashSet<>(dataFrame.select("property2").collectAsList());
        expectedRows.clear();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            MutableList<Object> fields1 = new MutableList<>();
            fields1.appendElem(3.0F);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
            MutableList<Object> fields2 = new MutableList<>();
            fields2.appendElem(8.0F);
            expectedRows.add(Row$.MODULE$.fromSeq(fields2));
        }
        assertEquals(expectedRows, results);
    }

    @Test
    public void checkGetCorrectElementsInDataFrameWithProjectionAndFiltering() throws OperationException {
        final Graph graph = getGraph("/schema-DataFrame/elements.json", getElements());

        // Get DataFrame
        final GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().edge(EDGE_GROUP).build())
                .build();
        final Dataset<Row> dataFrame = graph.execute(dfOperation, new User());

        // Check get correct rows when ask for all columns but only rows where property2 > 4.0
        Set<Row> results = new HashSet<>(dataFrame.filter("property2 > 4.0").collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final MutableList<Object> fields = new MutableList<>();
            fields.appendElem(EDGE_GROUP);
            fields.appendElem("" + i);
            fields.appendElem("C");
            fields.appendElem(true);
            fields.appendElem(null);
            fields.appendElem(6);
            fields.appendElem(7);
            fields.appendElem(8.0F);
            fields.appendElem(9.0D);
            fields.appendElem(10L);
            fields.appendElem(i * 200L);
            expectedRows.add(Row$.MODULE$.fromSeq(fields));
        }
        assertEquals(expectedRows, results);

        // Check get correct rows when ask for columns property2 and property3 but only rows where property2 > 4.0
        results = new HashSet<>(dataFrame.select("property2", "property3").filter("property2 > 4.0").collectAsList());
        expectedRows.clear();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final MutableList<Object> fields = new MutableList<>();
            fields.appendElem(8.0F);
            fields.appendElem(9.0D);
            expectedRows.add(Row$.MODULE$.fromSeq(fields));
        }
        assertEquals(expectedRows, results);
    }

    @Test
    public void checkGetExceptionIfIncompatibleSchemas() throws OperationException {
        final Graph graph = getGraph("/schema-DataFrame/elementsIncompatible.json", Collections.<Element>emptyList());

        // Use entity and edges group - check get correct data
        final GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().entity(ENTITY_GROUP).edge(EDGE_GROUP).build())
                .build();
        // NB Catch the exception rather than using expected annotation on test to ensure that the SparkContext
        // is shut down.
        try {
            graph.execute(dfOperation, new User());
            fail("IllegalArgumentException should have been thrown");
        } catch (final IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void checkCanDealWithNonStandardProperties() throws OperationException {
        final Graph graph = getGraph("/schema-DataFrame/elementsNonstandardTypes.json", getElementsWithNonStandardProperties());
        // Edges group - check get correct edges
        GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().edge(EDGE_GROUP).build())
                .build();
        Dataset<Row> dataFrame = graph.execute(dfOperation, new User());
        Set<Row> results = new HashSet<>(dataFrame.collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        final MutableList<Object> fields1 = new MutableList<>();
        Map<String, Long> freqMap = Map$.MODULE$.empty();
        freqMap.put("Y", 1000L);
        freqMap.put("Z", 10000L);
        fields1.appendElem(EDGE_GROUP);
        fields1.appendElem("B");
        fields1.appendElem("C");
        fields1.appendElem(true);
        fields1.appendElem(null);
        fields1.appendElem(freqMap);
        final HyperLogLogPlus hllpp = new HyperLogLogPlus(5, 5);
        hllpp.offer("AAA");
        hllpp.offer("BBB");
        fields1.appendElem(hllpp.cardinality());
        expectedRows.add(Row$.MODULE$.fromSeq(fields1));
        assertEquals(expectedRows, results);

        // Entities group - check get correct entities
        dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().entity(ENTITY_GROUP).build())
                .build();
        dataFrame = graph.execute(dfOperation, new User());
        results.clear();
        results.addAll(dataFrame.collectAsList());
        expectedRows.clear();
        fields1.clear();
        freqMap.clear();
        freqMap.put("W", 10L);
        freqMap.put("X", 100L);
        fields1.appendElem(ENTITY_GROUP);
        fields1.appendElem("A");
        fields1.appendElem(freqMap);
        final HyperLogLogPlus hllpp2 = new HyperLogLogPlus(5, 5);
        hllpp2.offer("AAA");
        fields1.appendElem(hllpp2.cardinality());
        expectedRows.add(Row$.MODULE$.fromSeq(fields1));
        assertEquals(expectedRows, results);
    }

    @Test
    public void checkCanDealWithUserDefinedConversion() throws OperationException {
        final Graph graph = getGraph("/schema-DataFrame/elementsUserDefinedConversion.json", getElementsForUserDefinedConversion());

        // Edges group - check get correct edges
        final List<Converter> converters = new ArrayList<>();
        converters.add(new MyPropertyConverter());
        GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().edge(EDGE_GROUP).build())
                .converters(converters)
                .build();
        Dataset<Row> dataFrame = graph.execute(dfOperation, new User());
        Set<Row> results = new HashSet<>(dataFrame.collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        final MutableList<Object> fields1 = new MutableList<>();
        Map<String, Long> freqMap = Map$.MODULE$.empty();
        freqMap.put("Y", 1000L);
        freqMap.put("Z", 10000L);
        fields1.appendElem(EDGE_GROUP);
        fields1.appendElem("B");
        fields1.appendElem("C");
        fields1.appendElem(true);
        fields1.appendElem(null);
        fields1.appendElem(freqMap);
        final HyperLogLogPlus hllpp = new HyperLogLogPlus(5, 5);
        hllpp.offer("AAA");
        hllpp.offer("BBB");
        fields1.appendElem(hllpp.cardinality());
        fields1.appendElem(50);
        expectedRows.add(Row$.MODULE$.fromSeq(fields1));
        assertEquals(expectedRows, results);

        // Entities group - check get correct entities
        dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder().entity(ENTITY_GROUP).build())
                .converters(converters)
                .build();
        dataFrame = graph.execute(dfOperation, new User());
        results.clear();
        results.addAll(dataFrame.collectAsList());
        expectedRows.clear();
        fields1.clear();
        freqMap.clear();
        freqMap.put("W", 10L);
        freqMap.put("X", 100L);
        fields1.appendElem(ENTITY_GROUP);
        fields1.appendElem("A");
        fields1.appendElem(freqMap);
        final HyperLogLogPlus hllpp2 = new HyperLogLogPlus(5, 5);
        hllpp2.offer("AAA");
        fields1.appendElem(hllpp2.cardinality());
        fields1.appendElem(10);
        expectedRows.add(Row$.MODULE$.fromSeq(fields1));
        assertEquals(expectedRows, results);
    }

    @Test
    public void checkViewIsRespected() throws OperationException {
        final Graph graph = getGraph("/schema-DataFrame/elements.json", getElements());

        // Edges group - check get correct edges
        GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder()
                        .edge(EDGE_GROUP, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(800L))
                                        .build())
                                .build())
                        .build())
                .build();
        Dataset<Row> dataFrame = graph.execute(dfOperation, new User());
        Set<Row> results = new HashSet<>(dataFrame.collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            if (i * 200L > 800L) {
                final MutableList<Object> fields2 = new MutableList<>();
                fields2.appendElem(EDGE_GROUP);
                fields2.appendElem("" + i);
                fields2.appendElem("C");
                fields2.appendElem(true);
                fields2.appendElem(null);
                fields2.appendElem(6);
                fields2.appendElem(7);
                fields2.appendElem(8.0F);
                fields2.appendElem(9.0D);
                fields2.appendElem(10L);
                fields2.appendElem(i * 200L);
                expectedRows.add(Row$.MODULE$.fromSeq(fields2));
            }
        }
        assertEquals(expectedRows, results);

        // Entities group - check get correct entities
        dfOperation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder()
                        .entity(ENTITY_GROUP, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select("property1")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
                        .build())
                .build();
        dataFrame = graph.execute(dfOperation, new User());
        results.clear();
        results.addAll(dataFrame.collectAsList());
        expectedRows.clear();
        for (int i = 2; i < NUM_ELEMENTS; i++) {
            final MutableList<Object> fields1 = new MutableList<>();
            fields1.clear();
            fields1.appendElem(ENTITY_GROUP);
            fields1.appendElem("" + i);
            fields1.appendElem(1);
            fields1.appendElem(i);
            fields1.appendElem(3.0F);
            fields1.appendElem(4.0D);
            fields1.appendElem(5L);
            fields1.appendElem(6);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
        }
        assertEquals(expectedRows, results);
    }

    private Graph getGraph(final String elementsSchema, final List<Element> elements) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getClass().getResourceAsStream(elementsSchema))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/types.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();
        graph.execute(new AddElements.Builder().input(elements).build(), new User());
        return graph;
    }

    static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final Entity entity = new Entity.Builder().group(TestGroups.ENTITY)
                    .vertex("" + i)
                    .property("columnQualifier", 1)
                    .property("property1", i)
                    .property("property2", 3.0F)
                    .property("property3", 4.0D)
                    .property("property4", 5L)
                    .property("count", 6L)
                    .build();

            final Edge edge1 = new Edge.Builder().group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("B")
                    .directed(true)
                    .property("columnQualifier", 1)
                    .property("property1", 2)
                    .property("property2", 3.0F)
                    .property("property3", 4.0D)
                    .property("property4", 5L)
                    .property("count", 100L)
                    .build();

            final Edge edge2 = new Edge.Builder().group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(true)
                    .property("columnQualifier", 6)
                    .property("property1", 7)
                    .property("property2", 8.0F)
                    .property("property3", 9.0D)
                    .property("property4", 10L)
                    .property("count", i * 200L)
                    .build();

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);
        }
        return elements;
    }

    private static List<Element> getElementsWithNonStandardProperties() {
        final List<Element> elements = new ArrayList<>();

        final FreqMap freqMap = new FreqMap();
        freqMap.put("W", 10L);
        freqMap.put("X", 100L);

        final HyperLogLogPlus hllpp = new HyperLogLogPlus(5, 5);
        hllpp.offer("AAA");

        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("A")
                .property("freqMap", freqMap)
                .property("hllpp", hllpp)
                .build();
        elements.add(entity);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("B")
                .dest("C")
                .directed(true)
                .build();
        final FreqMap freqMap2 = new FreqMap();
        freqMap2.put("Y", 1000L);
        freqMap2.put("Z", 10000L);
        edge.putProperty("freqMap", freqMap2);
        final HyperLogLogPlus hllpp2 = new HyperLogLogPlus(5, 5);
        hllpp2.offer("AAA");
        hllpp2.offer("BBB");
        edge.putProperty("hllpp", hllpp2);
        elements.add(edge);
        return elements;
    }

    private static List<Element> getElementsForUserDefinedConversion() {
        final List<Element> elements = new ArrayList<>();

        final FreqMap freqMap = new FreqMap();
        freqMap.put("W", 10L);
        freqMap.put("X", 100L);

        final HyperLogLogPlus hllpp = new HyperLogLogPlus(5, 5);
        hllpp.offer("AAA");

        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("A")
                .property("freqMap", freqMap)
                .property("hllpp", hllpp)
                .property("myProperty", new MyProperty(10))
                .build();
        elements.add(entity);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("B")
                .dest("C")
                .directed(true)
                .build();

        final FreqMap freqMap2 = new FreqMap();
        freqMap2.put("Y", 1000L);
        freqMap2.put("Z", 10000L);
        edge.putProperty("freqMap", freqMap2);

        final HyperLogLogPlus hllpp2 = new HyperLogLogPlus(5, 5);
        hllpp2.offer("AAA");
        hllpp2.offer("BBB");
        edge.putProperty("hllpp", hllpp2);
        edge.putProperty("myProperty", new MyProperty(50));
        elements.add(edge);
        return elements;
    }

    private static class MyPropertyConverter implements Converter {
        private static final long serialVersionUID = 7777521632508320165L;

        @Override
        public boolean canHandle(final Class clazz) {
            return MyProperty.class.equals(clazz);
        }

        @Override
        public DataType convertedType() {
            return DataTypes.IntegerType;
        }

        @Override
        public Object convert(final Object object) throws ConversionException {
            return ((MyProperty) object).getA();
        }
    }

}
