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

package uk.gov.gchq.gaffer.flink.operation.handler;

import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Element;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GafferSinkTest {
    @Test
    public void shouldDelegateOpenToGafferAddedInitialise() throws Exception {
        // Given
        final GafferAdder adder = mock(GafferAdder.class);
        final GafferSink sink = new GafferSink(adder);

        // When
        sink.open(null);

        // Then
        verify(adder).initialise();
    }

    @Test
    public void shouldDelegateInvokeToGafferAddedInitialise() throws Exception {
        // Given
        final GafferAdder adder = mock(GafferAdder.class);
        final GafferSink sink = new GafferSink(adder);
        final Element element = mock(Element.class);

        // When
        sink.invoke(element);

        // Then
        verify(adder).add(element);
    }
}
