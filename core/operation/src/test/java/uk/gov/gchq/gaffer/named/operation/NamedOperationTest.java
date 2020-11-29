/*
 * Copyright 2019-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.named.operation;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NamedOperationTest extends OperationTest<NamedOperation> {

    @Test
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final String testOpName = "testOpName";
        final Map testParamsMap = Collections.singletonMap("test", "testVal");
        NamedOperation op = (NamedOperation) new NamedOperation.Builder()
                .name(testOpName)
                .parameters(testParamsMap)
                .build();

        // When
        final byte[] json = toJson(op);
        final NamedOperation deserialisedOp = fromJson(json);

        // Then
        assertEquals(testOpName, deserialisedOp.getOperationName());
        assertEquals(testParamsMap, deserialisedOp.getParameters());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final String testOpName = "testOpName";
        final Map testParamsMap = Collections.singletonMap("test", "testVal");

        // When
        NamedOperation op = (NamedOperation) new NamedOperation.Builder()
                .name(testOpName)
                .parameters(testParamsMap)
                .build();

        // Then
        assertEquals(testOpName, op.getOperationName());
        assertEquals(testParamsMap, op.getParameters());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String testOpName = "testOpName";
        final Map testParamsMap = Collections.singletonMap("test", "testVal");
        NamedOperation op = (NamedOperation) new NamedOperation.Builder()
                .name(testOpName)
                .parameters(testParamsMap)
                .build();

        // When
        NamedOperation clonedOp = op.shallowClone();

        // Then
        assertEquals(testOpName, clonedOp.getOperationName());
        assertEquals(testParamsMap, clonedOp.getParameters());
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("operationName");
    }

    @Override
    protected NamedOperation getTestObject() {
        return new NamedOperation();
    }
}
