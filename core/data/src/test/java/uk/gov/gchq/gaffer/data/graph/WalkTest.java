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

package uk.gov.gchq.gaffer.data.graph;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class WalkTest {

    private static final Edge EDGE_AB = new Edge.Builder().group(TestGroups.EDGE).source("A").dest("B").directed(true).build();
    private static final Edge EDGE_BC = new Edge.Builder().group(TestGroups.EDGE).source("B").dest("C").directed(true).build();
    private static final Edge EDGE_CB = new Edge.Builder().group(TestGroups.EDGE).source("C").dest("B").directed(true).build();
    private static final Edge EDGE_ED = new Edge.Builder().group(TestGroups.EDGE).source("E").dest("D").directed(true).build();
    private static final Edge EDGE_DA = new Edge.Builder().group(TestGroups.EDGE).source("D").dest("A").directed(true).build();
    private static final Edge EDGE_AE = new Edge.Builder().group(TestGroups.EDGE).source("A").dest("E").directed(true).build();

    private static final Entity ENTITY_A = new Entity.Builder().group(TestGroups.ENTITY).vertex("A").build();
    private static final Entity ENTITY_B = new Entity.Builder().group(TestGroups.ENTITY).vertex("B").build();
    private static final Entity ENTITY_C = new Entity.Builder().group(TestGroups.ENTITY).vertex("C").build();
    private static final Entity ENTITY_D = new Entity.Builder().group(TestGroups.ENTITY).vertex("D").build();
    private static final Entity ENTITY_E = new Entity.Builder().group(TestGroups.ENTITY).vertex("E").build();

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws Exception {
        // Given
        final Walk walk = new Walk.Builder()
                .edge(EDGE_AB)
                .entity(ENTITY_B)
                .edge(EDGE_BC)
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(walk);
        final Walk deserialisedWalk = JSONSerialiser.deserialise(json, Walk.class);


        // Then
        assertThat(walk, is(equalTo(deserialisedWalk)));
        final String expected = "{" +
                "  \"edges\": [" +
                "  [" +
                "    {\"group\": \"BasicEdge\"," +
                "     \"source\": \"A\"," +
                "     \"destination\": \"B\"," +
                "     \"directed\": true," +
                "     \"properties\": {}," +
                "     \"class\": \"uk.gov.gchq.gaffer.data.element.Edge\"}" +
                "  ]," +
                "  [" +
                "    {\"group\": \"BasicEdge\"," +
                "     \"source\": \"B\"," +
                "     \"destination\": \"C\"," +
                "     \"directed\": true," +
                "     \"properties\": {}," +
                "     \"class\": \"uk.gov.gchq.gaffer.data.element.Edge\"}" +
                "    ]" +
                "  ]," +
                "  \"entities\": [" +
                "    {\"A\": []}," +
                "    {\"B\": [" +
                "      {\"group\": \"BasicEntity\"," +
                "      \"vertex\": \"B\"," +
                "      \"properties\": {}," +
                "      \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\"}]" +
                "    }," +
                "    {\"C\": []}" +
                "  ]" +
                "}\n";
                JsonAssert.assertEquals(expected, new String(json));
    }

    @Test
    public void shouldFailToAddEdgeWithInvalidEntitySource() {
        final Walk.Builder builder = new Walk.Builder().entity(ENTITY_A);

        final Exception exception = assertThrows(IllegalArgumentException.class, () -> builder.edge(EDGE_BC));
        assertEquals("Edge must continue the current walk.", exception.getMessage());
    }

    @Test
    public void shouldFailToAddEdgeWithInvalidEdgeSource() {
        final Walk.Builder builder = new Walk.Builder().edge(EDGE_AB);

        final Exception exception = assertThrows(IllegalArgumentException.class, () -> builder.edge(EDGE_AB));
        assertEquals("Edge must continue the current walk.", exception.getMessage());
    }

    @Test
    public void shouldFailToAddEntityWithInvalidEdgeSource() {
        final Walk.Builder builder = new Walk.Builder().edge(EDGE_AB);

        // When
        final Exception exception = assertThrows(IllegalArgumentException.class,
                () -> builder.entity(ENTITY_A));
        assertEquals("Entity must be added to correct vertex.", exception.getMessage());
    }

    @Test
    public void shouldFailToAddEntityWithInvalidEntitySource() {
        final Walk.Builder builder = new Walk.Builder().entity(ENTITY_A);

        final Exception exception = assertThrows(IllegalArgumentException.class, () -> builder.entity(ENTITY_B));
        assertEquals("Entity must be added to correct vertex.", exception.getMessage());
    }

    @Test
    public void shouldFailToAddEntitiesWithDifferentVertices() {
        final Walk.Builder builder = new Walk.Builder();

        final Exception exception = assertThrows(IllegalArgumentException.class, () -> builder.entities(ENTITY_A, ENTITY_B));
        assertEquals("Entities must all have the same vertex.", exception.getMessage());
    }

    @Test
    public void shouldFailToAddEntitiesWithInvalidEdgeSource() {
        final Walk.Builder builder = new Walk.Builder().edge(EDGE_AB);

        final Exception exception = assertThrows(IllegalArgumentException.class, () -> builder.entities(ENTITY_A, ENTITY_A));
        assertEquals("Entity must be added to correct vertex.", exception.getMessage());
    }

    @Test
    public void shouldFailToAddEntitiesWithInvalidEntitySource() {
        final Walk.Builder builder = new Walk.Builder().entity(ENTITY_A);

        final Exception exception = assertThrows(IllegalArgumentException.class, () -> builder.entities(ENTITY_B, ENTITY_B));
        assertEquals("Entity must be added to correct vertex.", exception.getMessage());
    }

    @Test
    public void shouldBuildWalkStartingWithEdge() {
        // Given
        // [A] -> [B] -> [C]
        //         \
        //          (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .edge(EDGE_AB)
                .entity(ENTITY_B)
                .edge(EDGE_BC)
                .build();

        // Then
        assertThat(walk.getEntitiesAsEntries(), hasSize(3)); // A, B, C
        assertThat(walk.getEdges(), hasSize(2)); // A -> B, B -> C
        assertThat(walk.getEdges().stream().flatMap(Set::stream).collect(Collectors.toList()), contains(EDGE_AB, EDGE_BC));
        assertThat(walk.getEntities(), contains(Collections.emptySet(), Sets.newHashSet(ENTITY_B), Collections.emptySet()));
        assertThat(walk.getVerticesOrdered(), contains("A", "B", "C"));
    }

    @Test
    public void shouldBuildWalkStartingWithEntity() {
        // Given
        // [A] -> [B] -> [C]
        //  \             \
        //   (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edges(EDGE_AB, EDGE_BC)
                .entity(ENTITY_C)
                .build();

        // Then
        assertThat(walk.getEntitiesAsEntries(), hasSize(3)); // A, B, C
        assertThat(walk.getEdges(), hasSize(2)); // A -> B, B -> C
        assertThat(walk.getEdges().stream().flatMap(Set::stream).collect(Collectors.toList()), contains(EDGE_AB, EDGE_BC));
        assertThat(walk.getEntities(), contains(Sets.newHashSet(ENTITY_A), Collections.emptySet(), Sets.newHashSet(ENTITY_C)));
        assertThat(walk.getVerticesOrdered(), contains("A", "B", "C"));
    }

    @Test
    public void shouldBuildWalkStartingWithEntities() {
        // Given
        // [A] -> [B] -> [C]
        //  \             \
        //   (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entities(ENTITY_A, ENTITY_A)
                .edges(EDGE_AB, EDGE_BC)
                .entity(ENTITY_C)
                .build();

        // Then
        assertThat(walk.getEntitiesAsEntries(), hasSize(3)); // A, B, C
        assertThat(walk.getEdges(), hasSize(2)); // A -> B, B -> C
        assertThat(walk.getEdges().stream().flatMap(Set::stream).collect(Collectors.toList()), contains(EDGE_AB, EDGE_BC));
        assertThat(walk.getEntities(), contains(Sets.newHashSet(ENTITY_A, ENTITY_A), Collections.emptySet(), Sets.newHashSet(ENTITY_C)));
        assertThat(walk.getVerticesOrdered(), contains("A", "B", "C"));
    }

    @Test
    public void shouldBuildWalkWithLoop() {
        // Given
        // [A] -> [E] -> [D] -> [A]
        //         \             \
        //          (BasicEntity) (BasicEntity, BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .edge(EDGE_AE)
                .entity(ENTITY_E)
                .edges(EDGE_ED, EDGE_DA)
                .entities(ENTITY_A, ENTITY_A)
                .build();

        // Then
        assertThat(walk.getEntitiesAsEntries(), hasSize(4)); // A, D, E, A
        assertThat(walk.getEdges(), hasSize(3)); // A -> E, E -> D, D -> A
        assertThat(walk.getEdges().stream().flatMap(Set::stream).collect(Collectors.toList()), contains(EDGE_AE, EDGE_ED, EDGE_DA));
        assertThat(walk.getEntities(), contains(Collections.emptySet(), Sets.newHashSet(ENTITY_E),
                Collections.emptySet(), Sets.newHashSet(ENTITY_A, ENTITY_A)));
        assertThat(walk.getVerticesOrdered(), contains("A", "E", "D", "A"));
    }

    @Test
    public void shouldAddEmptyIterableOfEntities() {
        // Given
        // [A] -> [E] -> [D] -> [A]
        //         \             \
        //          (BasicEntity) (EmptyIterable)

        // When
        final Walk walk = new Walk.Builder()
                .edge(EDGE_AE)
                .entity(ENTITY_E)
                .edges(EDGE_ED, EDGE_DA)
                .entities(new EmptyClosableIterable<>())
                .build();

        // Then
        assertThat(walk.getEntitiesAsEntries(), hasSize(4)); // A, D, E, A
        assertThat(walk.getEdges(), hasSize(3)); // A -> E, E -> D, D -> A
        assertThat(walk.getEdges().stream().flatMap(Set::stream).collect(Collectors.toList()), contains(EDGE_AE, EDGE_ED, EDGE_DA));
        assertThat(walk.getEntities(), contains(Collections.emptySet(), Sets.newHashSet(ENTITY_E),
                Collections.emptySet(), Collections.emptySet()));
        assertThat(walk.getVerticesOrdered(), contains("A", "E", "D", "A"));
    }

    @Test
    public void shouldGetEntitiesForVertices() {
        // Given
        // [A]     ->    [E]     ->    [D]
        //  \             \             \
        //   (BasicEntity) (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edge(EDGE_AE)
                .entities(ENTITY_E)
                .edge(EDGE_ED)
                .entity(ENTITY_D)
                .build();

        // Then
        assertThat(walk.getEntitiesForVertex("E"), hasSize(1));
        assertThat(walk.getEntitiesForVertex("E"), contains(ENTITY_E));
    }

    @Test
    public void shouldGetEntitiesAtDistance() {
        // Given
        // [A]     ->    [E]     ->    [D]
        //  \             \             \
        //   (BasicEntity) (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edge(EDGE_AE)
                .entities(ENTITY_E, ENTITY_E)
                .edge(EDGE_ED)
                .entity(ENTITY_D)
                .build();

        // Then
        assertThat(walk.getEntitiesAtDistance(2), hasSize(1));
        assertThat(walk.getEntitiesAtDistance(2), contains(ENTITY_D));
    }

    @Test
    public void shouldGetVertexSet() {
        // Given
        // [A]     ->    [E]     ->    [D]
        //  \             \             \
        //   (BasicEntity) (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edge(EDGE_AE)
                .entities(ENTITY_E, ENTITY_E)
                .edge(EDGE_ED)
                .entity(ENTITY_D)
                .build();

        // Then
        assertThat(walk.getVertexSet(), hasItems("A", "E", "D"));
    }

    @Test
    public void shouldGetLength() {
        // Given
        // [A]     ->    [E]     ->    [D]
        //  \             \             \
        //   (BasicEntity) (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edge(EDGE_AE)
                .entities(ENTITY_E, ENTITY_E)
                .edge(EDGE_ED)
                .entity(ENTITY_D)
                .build();

        // Then
        assertThat(walk.length(), is(2));
    }

    @Test
    public void shouldGetTrail() {
        // Given
        // [A]     ->    [E]     ->    [D]
        //  \             \             \
        //   (BasicEntity) (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edge(EDGE_AE)
                .entities(ENTITY_E, ENTITY_E)
                .edge(EDGE_ED)
                .entity(ENTITY_D)
                .build();

        // Then
        assertThat(walk.isTrail(), is(true));
    }

    @Test
    public void shouldGetNotTrail() {
        // Given
        // [A] -> [B] -> [C] -> [B] -> [C]

        // When
        final Walk walk = new Walk.Builder()
                .edge(EDGE_AB)
                .edge(EDGE_BC)
                .edge(EDGE_CB)
                .edge(EDGE_BC)
                .build();

        // Then
        assertThat(walk.isTrail(), is(false));
    }

    @Test
    public void shouldGetPath() {
        // Given
        // [A]     ->    [E]     ->    [D]
        //  \             \             \
        //   (BasicEntity) (BasicEntity) (BasicEntity)

        // When
        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edge(EDGE_AE)
                .entities(ENTITY_E, ENTITY_E)
                .edge(EDGE_ED)
                .entity(ENTITY_D)
                .build();

        // Then
        assertThat(walk.isPath(), is(true));
    }

    @Test
    public void shouldGetNotPath() {
        // Given
        // [A] -> [B] -> [C] -> [B]

        // When
        final Walk walk = new Walk.Builder()
                .edge(EDGE_AB)
                .edge(EDGE_BC)
                .edge(EDGE_CB)
                .build();

        // Then
        assertThat(walk.isPath(), is(false));
    }

    @Test
    public void shouldGetSourceVertexFromWalk() {
        // Given
        // [A] -> [B] -> [C]
        //  \             \
        //   (BasicEntity) (BasicEntity)

        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edges(EDGE_AB, EDGE_BC)
                .entity(ENTITY_C)
                .build();

        // When
        final Object result = walk.getSourceVertex();

        // Then
        assertEquals("A", result);
    }

    @Test
    public void shouldGetDestinationVertexFromWalk() {
        // Given
        // [A]     ->    [E]     ->    [D]
        //  \             \             \
        //   (BasicEntity) (BasicEntity) (BasicEntity)

        final Walk walk = new Walk.Builder()
                .entity(ENTITY_A)
                .edge(EDGE_AE)
                .entities(ENTITY_E)
                .edge(EDGE_ED)
                .entity(ENTITY_D)
                .build();

        // When
        final Object result = walk.getDestinationVertex();

        // Then
        assertEquals("D", result);
    }


    @Test
    public void shouldGetSourceVertexFromWalkWithNoEntities() {
        // Given
        // [A] -> [B] -> [C]

        final Walk walk = new Walk.Builder()
                .edges(EDGE_AB, EDGE_BC)
                .build();

        // When
        final Object result = walk.getSourceVertex();

        // Then
        assertEquals("A", result);
    }

    @Test
    public void shouldGetDestinationVertexFromWalkWithNoEntities() {
        // Given
        // [A] -> [B] -> [C]

        final Walk walk = new Walk.Builder()
                .edges(EDGE_AB, EDGE_BC)
                .build();

        // When
        final Object result = walk.getDestinationVertex();

        // Then
        assertEquals("C", result);
    }
}
