/*
 * Copyright 2016-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.implementation;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import java.util.HashSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TreeSetStringSerialiserTest extends ToBytesSerialisationTest<TreeSet<String>> {

    @Test
    public void shouldSerialiseAndDeserialiseATreeSet() throws SerialisationException {
        // Given
        final TreeSet<String> set = new TreeSet<>();
        set.add("string1");
        set.add("string2");
        set.add("string3");
        set.add("string4");

        // When
        final byte[] serialisedSet = serialiser.serialise(set);
        final TreeSet deserialisedSet = serialiser.deserialise(serialisedSet);

        // Then
        assertNotSame(deserialisedSet, set);
        assertEquals(deserialisedSet, set);
    }

    @Test
    public void shouldSerialiseAndDeserialiseAnEmptyTreeSet() throws SerialisationException {
        // Given
        final TreeSet<String> set = new TreeSet<>();

        // When
        final byte[] serialisedSet = serialiser.serialise(set);
        final TreeSet deserialisedSet = serialiser.deserialise(serialisedSet);

        // Then
        assertNotSame(deserialisedSet, set);
        assertEquals(deserialisedSet, set);
    }

    @Test
    public void shouldBeAbleToHandleATreeSet() {
        // Given
        final Class testClass = TreeSet.class;

        // When
        final boolean canHandle = serialiser.canHandle(testClass);

        // Then
        assertTrue(canHandle);
    }

    @Test
    public void shouldNotBeAbleToHandleAHashSet() {
        // Given
        final Class testClass = HashSet.class;

        // When
        final boolean canHandle = serialiser.canHandle(testClass);

        // Then
        assertFalse(canHandle);
    }


    @Test
    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        // Given When
        final TreeSet<String> tree = serialiser.deserialiseEmpty();

        // Then
        assertNotNull(tree);
        assertTrue(tree.isEmpty());
    }

    @Override
    public Serialiser<TreeSet<String>, byte[]> getSerialisation() {
        return new TreeSetStringSerialiser();
    }

    @Override
    public Pair<TreeSet<String>, byte[]>[] getHistoricSerialisationPairs() {
        final TreeSet<String> set = Sets.newTreeSet();
        set.add("this");
        set.add("is");
        set.add("S P A R T A!!!!!");
        return new Pair[] {new Pair<>(set, new byte[] {123, 83, 32, 80, 32, 65, 32, 82, 32, 84, 32, 65, 33, 33, 33, 33, 33, 92, 44, 105, 115, 92, 44, 116, 104, 105, 115, 125})};
    }
}
