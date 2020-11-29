/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.IntegerParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.StringParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils.getParquetStoreProperties;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;

public class ParquetStoreTest {

    private static final String VERTEX = "vertex";
    private final Schema schema = new Schema.Builder()
            .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .build())
            .type(TestTypes.PROP_INTEGER, Integer.class)
            .type(DIRECTED_EITHER, Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source(TestTypes.ID_STRING)
                    .destination(TestTypes.ID_STRING)
                    .directed(DIRECTED_EITHER)
                    .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                    .aggregate(false)
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                    .vertex(TestTypes.ID_STRING)
                    .aggregate(false)
                    .build())
            .build();
    private final Edge unknownEdge = new Edge.Builder()
            .group(TestGroups.EDGE_2)
            .source("X")
            .dest("Y")
            .directed(false)
            .property(TestPropertyNames.PROP_1, 2)
            .build();
    private final Entity unknownEntity = new Entity.Builder()
            .vertex(VERTEX)
            .group(TestGroups.ENTITY_2)
            .property(TestPropertyNames.PROP_1, 2)
            .build();
    private final Entity knownEntity = new Entity.Builder()
            .vertex(VERTEX)
            .group(TestGroups.ENTITY)
            .property(TestPropertyNames.PROP_1, 2)
            .build();

    @Test
    public void testTraits() throws StoreException {
        final ParquetStore store = new ParquetStore();
        final Set<StoreTrait> expectedTraits = new HashSet<>();
        expectedTraits.add(StoreTrait.INGEST_AGGREGATION);
        expectedTraits.add(StoreTrait.PRE_AGGREGATION_FILTERING);
        expectedTraits.add(StoreTrait.ORDERED);
//        expectedTraits.add(StoreTrait.STORE_VALIDATION);
//        expectedTraits.add(StoreTrait.VISIBILITY);
        assertEquals(expectedTraits, store.getTraits());
    }

    @Test
    public void testMissingDataDirectory() {
        // Given
        final ParquetStoreProperties properties = new ParquetStoreProperties();
        properties.setTempFilesDir("/tmp/tmpdata");

        // When / Then
        try {
            ParquetStore.createStore("G", TestUtils.gafferSchema("schemaUsingStringVertexType"), properties);
        } catch (final IllegalArgumentException e) {
            // Expected
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    @Test
    public void testMissingTmpDataDirectory() {
        // Given
        final ParquetStoreProperties properties = new ParquetStoreProperties();
        properties.setDataDir("/tmp/data");

        // When / Then
        try {
            ParquetStore.createStore("G", TestUtils.gafferSchema("schemaUsingStringVertexType"), properties);
        } catch (final IllegalArgumentException e) {
            // Expected
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    @Test
    public void shouldFailSettingSnapshotWhenSnapshotNotExists(@TempDir java.nio.file.Path tempDir)
            throws IOException {
        //Given
        final ParquetStoreProperties properties = getParquetStoreProperties(tempDir);
        ParquetStore store = (ParquetStore)
                ParquetStore.createStore("G", TestUtils.gafferSchema("schemaUsingStringVertexType"), properties);

        // When / Then
        try {
            store.setLatestSnapshot(12345L);
        } catch (StoreException e) {
            //Expected
            assertThat(e.getMessage(), containsString("does not exist"));
            return;
        }
        fail("StoreException should have been thrown as folder already exists");
    }

    @Test
    public void shouldNotFailSettingSnapshotWhenSnapshotExists(@TempDir java.nio.file.Path tempDir)
            throws IOException {
        //Given
        final ParquetStoreProperties properties = getParquetStoreProperties(tempDir);
        ParquetStore store = (ParquetStore)
                ParquetStore.createStore("G", TestUtils.gafferSchema("schemaUsingStringVertexType"), properties);
        Files.createDirectories(tempDir.resolve("data").resolve(ParquetStore.getSnapshotPath(12345L)));

        // When / Then
        try {
            store.setLatestSnapshot(12345L);
        } catch (StoreException e) {
            fail("StoreException should not have been thrown. Message is:\n" + e.getMessage());
        }
    }

    @Test
    public void shouldNotThrowExceptionWhenAddingASingleEdgeWithGroupNotInSchema(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        getGraph(tempDir).execute(new AddElements.Builder()
                        .input(unknownEdge)
                        .build(),
                new User());
    }

    @Test
    public void shouldNotThrowExceptionWhenAddingASingleEntityWithGroupNotInSchema(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        getGraph(tempDir).execute(new AddElements.Builder()
                        .input(unknownEntity)
                        .build(),
                new User());
    }

    @Test
    public void shouldNotThrowExceptionWhenAddingAMultipleElementsWithGroupsNotInSchema(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        getGraph(tempDir).execute(new AddElements.Builder()
                        .input(unknownEntity, unknownEdge)
                        .build(),
                new User());
    }

    @Test
    public void shouldAddElementWhenAddingBothValidAndInvalidElementsWithoutException(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        final Graph graph = getGraph(tempDir);
        graph.execute(new AddElements.Builder()
                        .input(knownEntity, unknownEntity)
                        .build(),
                new User());

        Iterable<? extends Element> results = graph.execute(new GetAllElements(), new User());
        Iterator<? extends Element> iter = results.iterator();

        assertEquals(1, Iterables.size(results));
        assertTrue(iter.hasNext());
        assertEquals(knownEntity, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void shouldCorrectlyUseCompressionOption(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        for (final String compressionType : Sets.newHashSet("GZIP", "SNAPPY", "UNCOMPRESSED")) {
            // Given
            final Schema schema = new Schema.Builder()
                    .type("int", new TypeDefinition.Builder()
                            .clazz(Integer.class)
                            .serialiser(new IntegerParquetSerialiser())
                            .build())
                    .type("string", new TypeDefinition.Builder()
                            .clazz(String.class)
                            .serialiser(new StringParquetSerialiser())
                            .build())
                    .type(DIRECTED_EITHER, Boolean.class)
                    .entity("entity", new SchemaEntityDefinition.Builder()
                            .vertex("string")
                            .property("property1", "int")
                            .aggregate(false)
                            .build())
                    .edge("edge", new SchemaEdgeDefinition.Builder()
                            .source("string")
                            .destination("string")
                            .property("property2", "int")
                            .directed(DIRECTED_EITHER)
                            .aggregate(false)
                            .build())
                    .vertexSerialiser(new StringParquetSerialiser())
                    .build();
            final ParquetStoreProperties parquetStoreProperties = TestUtils.getParquetStoreProperties(tempDir);
            parquetStoreProperties.setCompressionCodecName(compressionType);
            final ParquetStore parquetStore =
                    (ParquetStore) ParquetStore.createStore("graphId", schema, parquetStoreProperties);
            final List<Element> elements = new ArrayList<>();
            elements.add(new Entity.Builder()
                    .group("entity")
                    .vertex("A")
                    .property("property1", 1)
                    .build());
            elements.add(new Edge.Builder()
                    .group("edge")
                    .source("B")
                    .dest("C")
                    .property("property2", 100)
                    .build());

            // When
            final AddElements add = new AddElements.Builder().input(elements).build();
            parquetStore.execute(add, new Context());

            // Then
            final List<Path> files = parquetStore.getFilesForGroup("entity");
            for (final Path path : files) {
                final ParquetMetadata parquetMetadata = ParquetFileReader
                        .readFooter(new Configuration(), path, ParquetMetadataConverter.NO_FILTER);
                for (final BlockMetaData blockMetadata : parquetMetadata.getBlocks()) {
                    blockMetadata.getColumns().forEach(c -> assertEquals(compressionType, c.getCodec().name()));
                }
            }
        }
    }

    private Graph getGraph(final java.nio.file.Path tempDir) throws IOException {
        return new Graph.Builder()
                .addSchema(schema)
                .storeProperties(getParquetStoreProperties(tempDir))
                .config(new GraphConfig.Builder().graphId("testGraphId").build())
                .build();
    }
}
