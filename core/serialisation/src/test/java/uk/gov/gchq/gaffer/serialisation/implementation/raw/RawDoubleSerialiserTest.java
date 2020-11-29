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

public class RawDoubleSerialiserTest extends ToBytesSerialisationTest<Double> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        // Given
        for (double i = 0; i < 1000; i += 1.1) {
            // When
            byte[] b = serialiser.serialise(i);
            Object o = serialiser.deserialise(b);

            // Then
            assertEquals(Double.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseDoubleMinValue() throws SerialisationException {
        // Given
        byte[] b = serialiser.serialise(Double.MIN_VALUE);
        Object o = serialiser.deserialise(b);

        // Then
        assertEquals(Double.class, o.getClass());
        assertEquals(Double.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseDoubleMaxValue() throws SerialisationException {
        // Given When
        byte[] b = serialiser.serialise(Double.MAX_VALUE);
        Object o = serialiser.deserialise(b);

        // Then
        assertEquals(Double.class, o.getClass());
        assertEquals(Double.MAX_VALUE, o);
    }

    @Test
    public void cantSerialiseStringClass() {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseDoubleClass() {
        assertTrue(serialiser.canHandle(Double.class));
    }

    @Override
    public Serialiser<Double, byte[]> getSerialisation() {
        return new RawDoubleSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Double, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[] {
                new Pair<>(Double.MAX_VALUE, new byte[] {-1, -1, -1, -1, -1, -1, -17, 127}),
                new Pair<>(Double.MIN_VALUE, new byte[] {1, 0, 0, 0, 0, 0, 0, 0}),
                new Pair<>(0.0, new byte[] {0, 0, 0, 0, 0, 0, 0, 0}),
                new Pair<>(1.00, new byte[] {0, 0, 0, 0, 0, 0, -16, 63}),
        };
    }
}
