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

package uk.gov.gchq.gaffer.types.function;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.koryphe.function.FunctionTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ToTypeSubTypeValueTest extends FunctionTest {

    @Test
    public void shouldConvertStringToTypeSubTypeValue() {
        // Given
        final ToTypeSubTypeValue function = new ToTypeSubTypeValue();
        final String value = "value1";

        // When
        final TypeSubTypeValue result = function.apply(value);

        // Then
        assertEquals(new TypeSubTypeValue(null, null, value), result);
    }

    @Test
    public void shouldConvertObjectToTypeSubTypeValue() {
        // Given
        final ToTypeSubTypeValue function = new ToTypeSubTypeValue();
        final Object value = 1L;

        // When
        final TypeSubTypeValue result = function.apply(value);

        // Then
        assertEquals(new TypeSubTypeValue(null, null, value.toString()), result);
    }

    @Test
    public void shouldConvertNullToTypeSubTypeValue() {
        // Given
        final ToTypeSubTypeValue function = new ToTypeSubTypeValue();

        // When
        final TypeSubTypeValue result = function.apply(null);

        // Then
        assertEquals(new TypeSubTypeValue(null, null, null), result);
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ToTypeSubTypeValue function = new ToTypeSubTypeValue();

        // When 1
        final String json = new String(JSONSerialiser.serialise(function, true));

        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.function.ToTypeSubTypeValue\"%n" +
                "}"), json);

        // When 2
        final ToTypeSubTypeValue deserialisedFunction = JSONSerialiser.deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedFunction);
    }

    @Override
    protected ToTypeSubTypeValue getInstance() {
        return new ToTypeSubTypeValue();
    }

    @Override
    protected Iterable<ToTypeSubTypeValue> getDifferentInstancesOrNull() {
        return null;
    }

    @Override
    protected Class<ToTypeSubTypeValue> getFunctionClass() {
        return ToTypeSubTypeValue.class;
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Object.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{TypeSubTypeValue.class};
    }
}
