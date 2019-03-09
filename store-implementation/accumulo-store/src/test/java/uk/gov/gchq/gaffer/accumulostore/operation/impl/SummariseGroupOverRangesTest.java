/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloTestData;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class SummariseGroupOverRangesTest extends OperationTest<SummariseGroupOverRanges> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final List<Pair<ElementId, ElementId>> pairList = new ArrayList<>();
        final Pair<ElementId, ElementId> pair1 = new Pair<>(AccumuloTestData.SEED_SOURCE_1, AccumuloTestData.SEED_DESTINATION_1);
        final Pair<ElementId, ElementId> pair2 = new Pair<>(AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2);
        pairList.add(pair1);
        pairList.add(pair2);
        final SummariseGroupOverRanges op = new SummariseGroupOverRanges.Builder()
                .input(pairList)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final SummariseGroupOverRanges deserialisedOp = JSONSerialiser.deserialise(json, SummariseGroupOverRanges.class);

        // Then
        final Iterator<? extends Pair<? extends ElementId, ? extends ElementId>> itrPairs = deserialisedOp.getInput().iterator();
        assertEquals(pair1, itrPairs.next());
        assertEquals(pair2, itrPairs.next());
        assertFalse(itrPairs.hasNext());

    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Pair<ElementId, ElementId> seed = new Pair<>(AccumuloTestData.SEED_A, AccumuloTestData.SEED_B);
        final SummariseGroupOverRanges summariseGroupOverRanges = new SummariseGroupOverRanges.Builder()
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .input(seed)
                .directedType(DirectedType.UNDIRECTED)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(new View.Builder().edge("testEdgeGroup").build())
                .build();

        // When / Then
        assertEquals("true", summariseGroupOverRanges.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER, summariseGroupOverRanges.getIncludeIncomingOutGoing());
        assertEquals(DirectedType.UNDIRECTED, summariseGroupOverRanges.getDirectedType());
        assertEquals(seed, summariseGroupOverRanges.getInput().iterator().next());
        assertNotNull(summariseGroupOverRanges.getView());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Pair<ElementId, ElementId> seed = new Pair<>(AccumuloTestData.SEED_A, AccumuloTestData.SEED_B);
        final View view = new View.Builder().edge("testEdgeGroup").build();
        final SummariseGroupOverRanges summariseGroupOverRanges = new SummariseGroupOverRanges.Builder()
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .input(seed)
                .directedType(DirectedType.UNDIRECTED)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(view)
                .build();

        // When
        final SummariseGroupOverRanges clone = summariseGroupOverRanges.shallowClone();

        // Then
        assertNotSame(summariseGroupOverRanges, clone);
        assertEquals("true", clone.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER, clone.getIncludeIncomingOutGoing());
        assertEquals(DirectedType.UNDIRECTED, clone.getDirectedType());
        assertEquals(seed, clone.getInput().iterator().next());
        assertEquals(view, clone.getView());

    }

    @Override
    protected SummariseGroupOverRanges getTestObject() {
        return new SummariseGroupOverRanges();
    }
}
