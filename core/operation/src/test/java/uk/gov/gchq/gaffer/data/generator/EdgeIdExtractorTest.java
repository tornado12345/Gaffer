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

package uk.gov.gchq.gaffer.data.generator;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.operation.data.generator.EdgeIdExtractor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EdgeIdExtractorTest {
    @Test
    public void shouldGetIdentifierFromEdge() {
        // Given
        final EdgeIdExtractor extractor = new EdgeIdExtractor();
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("source")
                .dest("destination")
                .directed(true)
                .build();

        // When
        final EdgeId seed = extractor._apply(edge);

        // Then
        assertEquals("source", seed.getSource());
        assertEquals("destination", seed.getDestination());
        assertTrue(seed.isDirected());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEntity() {
        // Given
        final EdgeIdExtractor extractor = new EdgeIdExtractor();
        final Entity entity = new Entity(TestGroups.ENTITY, "identifier");

        // When / Then
        try {
            extractor._apply(entity);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e);
        }
    }
}
