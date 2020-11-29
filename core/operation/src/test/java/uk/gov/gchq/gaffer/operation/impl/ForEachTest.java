/*
 * Copyright 2018-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class ForEachTest extends OperationTest<ForEach> {

    final Iterable<EntitySeed> inputIterable = Arrays.asList(new EntitySeed("1"), new EntitySeed("2"));
    final Operation op = new GetElements();

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final ForEach<Object, Object> forEachOp = new ForEach.Builder<>()
                .input(inputIterable)
                .operation(op)
                .build();


        // Then
        assertThat(forEachOp.getInput(), is(notNullValue()));
        assertEquals(inputIterable, forEachOp.getInput());
        assertEquals(op, forEachOp.getOperation());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final ForEach forEachOp = getTestObject();

        // When
        final ForEach clone = forEachOp.shallowClone();

        // Then
        assertNotSame(forEachOp, clone);
        assertEquals(forEachOp.getInput(), clone.getInput());
        assertEquals(forEachOp.getOperation(), clone.getOperation());
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final ForEach obj = new ForEach.Builder<>()
                .input(inputIterable)
                .operation(op)
                .build();

        // When
        final byte[] json = toJson(obj);
        final ForEach deserialisedObj = fromJson(json);

        // Then
        final String expected = String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.operation.impl.ForEach\",%n" +
                "  \"input\" : [ {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.data.EntitySeed\",%n" +
                "    \"vertex\" : \"1\"%n" +
                "  }, {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.data.EntitySeed\",%n" +
                "    \"vertex\" : \"2\"%n" +
                "  } ],%n" +
                "  \"operation\" : {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.impl.get.GetElements\"%n" +
                "  }%n" +
                "}");
        JsonAssert.assertEquals(expected, new String(json));
        assertNotNull(deserialisedObj);
    }

    @Override
    protected ForEach<Object, Object> getTestObject() {
        return new ForEach<>();
    }
}
