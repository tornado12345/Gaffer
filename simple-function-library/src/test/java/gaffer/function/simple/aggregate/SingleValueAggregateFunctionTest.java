/*
 * Copyright 2016 Crown Copyright
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

package gaffer.function.simple.aggregate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import gaffer.function.AggregateFunctionTest;
import gaffer.function.Function;
import java.io.IOException;

public class SingleValueAggregateFunctionTest extends AggregateFunctionTest {

    public void shouldThrowExceptionIfDifferentValuesTryToBeAggregatedTogether() throws IOException {
        // Given
        final SingleValueAggregateFunction function = new SingleValueAggregateFunction();
        function._aggregate("input1");

        // When / Then
        try {
            function._aggregate("input2");
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    public void shouldAggregateTheSameValueTogether() throws IOException {
        // Given
        final SingleValueAggregateFunction function = new SingleValueAggregateFunction();
        final String value = "input1";

        // When
        function._aggregate(value);
        function._aggregate(value);

        final Object state = function._state();

        // Then
        assertEquals(value, state);
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final SingleValueAggregateFunction function = new SingleValueAggregateFunction();

        // When
        final String json = serialise(function);

        // Then
        assertEquals("{\"class\":\"gaffer.function.simple.aggregate.SingleValueAggregateFunction\"}", json);

        // When 2
        final SingleValueAggregateFunction deserialisedFunction = (SingleValueAggregateFunction) deserialise(json);

        // Then 2
        assertNotNull(deserialisedFunction);
    }

    @Override
    protected SingleValueAggregateFunction getInstance() {
        return new SingleValueAggregateFunction();
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return SingleValueAggregateFunction.class;
    }
}
