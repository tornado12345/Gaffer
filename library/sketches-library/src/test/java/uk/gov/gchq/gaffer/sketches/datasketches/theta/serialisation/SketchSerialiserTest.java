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
package uk.gov.gchq.gaffer.sketches.datasketches.theta.serialisation;

import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SketchSerialiserTest {
    private static final double DELTA = 0.01D;
    private static final SketchSerialiser SERIALISER = new SketchSerialiser();

    @Test
    public void testSerialiseAndDeserialise() {
        final UpdateSketch sketch = UpdateSketch.builder().build();
        sketch.update(1.0D);
        sketch.update(2.0D);
        sketch.update(3.0D);
        testSerialiser(sketch);

        final UpdateSketch emptySketch = UpdateSketch.builder().build();
        testSerialiser(emptySketch);
    }

    private void testSerialiser(final Sketch sketch) {
        final double estimate = sketch.getEstimate();
        final byte[] unionSerialised;
        try {
            unionSerialised = SERIALISER.serialise(sketch);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }

        final Sketch sketchDeserialised;
        try {
            sketchDeserialised = SERIALISER.deserialise(unionSerialised);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
            return;
        }
        assertEquals(estimate, sketchDeserialised.getEstimate(), DELTA);
    }

    @Test
    public void testCanHandleUnion() {
        assertTrue(SERIALISER.canHandle(Sketch.class));
        assertFalse(SERIALISER.canHandle(String.class));
    }

}
