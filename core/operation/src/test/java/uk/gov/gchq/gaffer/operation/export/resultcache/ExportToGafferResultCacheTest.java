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

package uk.gov.gchq.gaffer.operation.export.resultcache;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;


public class ExportToGafferResultCacheTest extends OperationTest<ExportToGafferResultCache> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final String key = "key";
        final HashSet<String> opAuths = Sets.newHashSet("1", "2");
        final ExportToGafferResultCache op = new ExportToGafferResultCache.Builder<>()
                .opAuths(opAuths)
                .key(key)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final ExportToGafferResultCache deserialisedOp = JSONSerialiser.deserialise(json, ExportToGafferResultCache.class);

        // Then
        assertEquals(key, deserialisedOp.getKey());
        assertEquals(opAuths, deserialisedOp.getOpAuths());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // When
        final String key = "key";
        final HashSet<String> opAuths = Sets.newHashSet("1", "2");
        final ExportToGafferResultCache op = new ExportToGafferResultCache.Builder<>()
                .opAuths(opAuths)
                .key(key)
                .build();

        // Then
        assertEquals(key, op.getKey());
        assertEquals(opAuths, op.getOpAuths());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String key = "key";
        final HashSet<String> opAuths = Sets.newHashSet("1", "2");
        final String input = "input";
        final ExportToGafferResultCache exportToGafferResultCache = new ExportToGafferResultCache.Builder<>()
                .key(key)
                .opAuths(opAuths)
                .input(input)
                .build();

        // When
        ExportToGafferResultCache clone = exportToGafferResultCache.shallowClone();

        // Then
        assertNotSame(exportToGafferResultCache, clone);
        assertEquals(key, clone.getKey());
        assertEquals(input, clone.getInput());
        assertEquals(opAuths, clone.getOpAuths());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Object.class, outputClass);
    }

    @Override
    protected ExportToGafferResultCache getTestObject() {
        return new ExportToGafferResultCache();
    }
}

