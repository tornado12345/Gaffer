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

package uk.gov.gchq.gaffer.hbasestore;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.HyperLogLogPlusSerialiser;
import uk.gov.gchq.gaffer.store.SerialisationFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HBaseSerialisationFactoryTest {

    @Test
    public void shouldReturnCustomSerialiserForCustomClass() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new HBaseSerialisationFactory();
        final Class<?> clazz = HyperLogLogPlus.class;

        // When
        final Serialiser serialiser = factory.getSerialiser(clazz);

        // Then
        assertTrue(serialiser.canHandle(clazz));
        assertEquals(HyperLogLogPlusSerialiser.class, serialiser.getClass());
    }

}
