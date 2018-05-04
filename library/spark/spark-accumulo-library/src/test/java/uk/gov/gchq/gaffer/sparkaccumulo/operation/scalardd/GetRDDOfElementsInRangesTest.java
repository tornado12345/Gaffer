package uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd;

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

public class GetRDDOfElementsInRangesTest extends OperationTest<GetRDDOfElementsInRanges> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final List<Pair<ElementId, ElementId>> pairList = new ArrayList<>();
        final Pair<ElementId, ElementId> pair1 = new Pair<>(AccumuloTestData.SEED_SOURCE_1, AccumuloTestData.SEED_DESTINATION_1);
        final Pair<ElementId, ElementId> pair2 = new Pair<>(AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2);
        pairList.add(pair1);
        pairList.add(pair2);
        final GetRDDOfElementsInRanges op = new GetRDDOfElementsInRanges.Builder()
                .input(pairList)
                .build();
        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final GetRDDOfElementsInRanges deserialisedOp = JSONSerialiser.deserialise(json, GetRDDOfElementsInRanges.class);

        // Then
        final Iterator<? extends Pair<? extends ElementId, ? extends ElementId>> itrPairs = deserialisedOp.getInput().iterator();
        assertEquals(pair1, itrPairs.next());
        assertEquals(pair2, itrPairs.next());
        assertFalse(itrPairs.hasNext());
    }

    @SuppressWarnings("unchecked")
    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final Pair<ElementId, ElementId> seed = new Pair<>(AccumuloTestData.SEED_A, AccumuloTestData.SEED_B);
        final GetRDDOfElementsInRanges GetRDDOfElementsInRanges = new GetRDDOfElementsInRanges.Builder()
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .input(seed)
                .directedType(DirectedType.UNDIRECTED)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(new View.Builder().edge("testEdgeGroup").build())
                .build();
        assertEquals("true", GetRDDOfElementsInRanges.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER, GetRDDOfElementsInRanges.getIncludeIncomingOutGoing());
        assertEquals(DirectedType.UNDIRECTED, GetRDDOfElementsInRanges.getDirectedType());
        assertEquals(seed, GetRDDOfElementsInRanges.getInput().iterator().next());
        assertNotNull(GetRDDOfElementsInRanges.getView());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Pair<ElementId, ElementId> seed = new Pair<>(AccumuloTestData.SEED_A, AccumuloTestData.SEED_B);
        final View view = new View.Builder().edge("testEdgeGroup").build();
        final GetRDDOfElementsInRanges GetRDDOfElementsInRanges = new GetRDDOfElementsInRanges.Builder()
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .input(seed)
                .directedType(DirectedType.UNDIRECTED)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(view)
                .build();

        // When
        final GetRDDOfElementsInRanges clone = GetRDDOfElementsInRanges.shallowClone();

        // Then
        assertNotSame(GetRDDOfElementsInRanges, clone);
        assertEquals("true", clone.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER, clone.getIncludeIncomingOutGoing());
        assertEquals(DirectedType.UNDIRECTED, clone.getDirectedType());
        assertEquals(seed, clone.getInput().iterator().next());
        assertEquals(view, clone.getView());
    }

    @Override
    protected GetRDDOfElementsInRanges getTestObject() {
        return new GetRDDOfElementsInRanges();
    }
}
