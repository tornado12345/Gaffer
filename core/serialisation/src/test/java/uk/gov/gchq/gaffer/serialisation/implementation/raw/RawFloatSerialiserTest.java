/*
 * Copyright 2016-2018 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RawFloatSerialiserTest extends ToBytesSerialisationTest<Float> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (float i = 0; i < 1000; i += 1.1) {
            byte[] b = serialiser.serialise(i);
            Object o = serialiser.deserialise(b);
            assertEquals(Float.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseFloatMinValue() throws SerialisationException {
        byte[] b = serialiser.serialise(Float.MIN_VALUE);
        Object o = serialiser.deserialise(b);
        assertEquals(Float.class, o.getClass());
        assertEquals(Float.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseFloatMaxValue() throws SerialisationException {
        byte[] b = serialiser.serialise(Float.MAX_VALUE);
        Object o = serialiser.deserialise(b);
        assertEquals(Float.class, o.getClass());
        assertEquals(Float.MAX_VALUE, o);
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseFloatClass() throws SerialisationException {
        assertTrue(serialiser.canHandle(Float.class));
    }

    @Override
    public Serialiser<Float, byte[]> getSerialisation() {
        return new RawFloatSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Float, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair<>(Float.MAX_VALUE, new byte[]{-1, -1, 127, 127}),
                new Pair<>(Float.MIN_VALUE, new byte[]{1, 0, 0, 0}),
                new Pair<>(0f, new byte[]{0, 0, 0, 0}),
                new Pair<>(1f, new byte[]{0, 0, -128, 63})
        };
    }
}
