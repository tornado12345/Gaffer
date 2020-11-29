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
package uk.gov.gchq.gaffer.serialisation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntegerSerialiserTest extends ToBytesSerialisationTest<Integer> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        // Given
        for (int i = 0; i < 1000; i++) {
            // When
            final byte[] b = serialiser.serialise(i);
            final Object o = serialiser.deserialise(b);

            // Then
            assertEquals(Integer.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseIntegerMinValue() throws SerialisationException {
        // Given When
        final byte[] b = serialiser.serialise(Integer.MIN_VALUE);
        final Object o = serialiser.deserialise(b);

        // Then
        assertEquals(Integer.class, o.getClass());
        assertEquals(Integer.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseIntegerMaxValue() throws SerialisationException {
        // Given When
        final byte[] b = serialiser.serialise(Integer.MAX_VALUE);
        final Object o = serialiser.deserialise(b);

        // Then
        assertEquals(Integer.class, o.getClass());
        assertEquals(Integer.MAX_VALUE, o);
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseIntegerClass() {
        assertTrue(serialiser.canHandle(Integer.class));
    }

    @Override
    public Serialiser<Integer, byte[]> getSerialisation() {
        return new IntegerSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Integer, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[] {
                new Pair<>(Integer.MAX_VALUE, new byte[] {50, 49, 52, 55, 52, 56, 51, 54, 52, 55}),
                new Pair<>(Integer.MIN_VALUE, new byte[] {45, 50, 49, 52, 55, 52, 56, 51, 54, 52, 56}),
                new Pair<>(0, new byte[] {48}),
                new Pair<>(1, new byte[] {49})
        };
    }
}
