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
package uk.gov.gchq.gaffer.serialisation.implementation.raw;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RawLongSerialiserTest extends ToBytesSerialisationTest<Long> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        // Given
        for (long i = 0; i < 1000; i++) {
            // When
            byte[] b = serialiser.serialise(i);
            Object o = serialiser.deserialise(b);

            // Then
            assertEquals(Long.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseLongMinValue() throws SerialisationException {
        // Given When
        byte[] b = serialiser.serialise(Long.MIN_VALUE);
        Object o = serialiser.deserialise(b);

        // Then
        assertEquals(Long.class, o.getClass());
        assertEquals(Long.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseLongMaxValue() throws SerialisationException {
        // Given When
        byte[] b = serialiser.serialise(Long.MAX_VALUE);
        Object o = serialiser.deserialise(b);

        // Then
        assertEquals(Long.class, o.getClass());
        assertEquals(Long.MAX_VALUE, o);
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseLongClass() {
        assertTrue(serialiser.canHandle(Long.class));
    }


    @Override
    public Serialiser<Long, byte[]> getSerialisation() {
        return new RawLongSerialiser();
    }


    @Override
    @SuppressWarnings("unchecked")
    public Pair<Long, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[] {
                new Pair<>(Long.MAX_VALUE, new byte[] {-1, -1, -1, -1, -1, -1, -1, 127}),
                new Pair<>(Long.MIN_VALUE, new byte[] {0, 0, 0, 0, 0, 0, 0, -128}),
                new Pair<>(0L, new byte[] {0, 0, 0, 0, 0, 0, 0, 0}),
                new Pair<>(1L, new byte[] {1, 0, 0, 0, 0, 0, 0, 0})
        };
    }
}
