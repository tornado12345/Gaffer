/*
 * Copyright 2016-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyRangeFactory;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ClassicRangeFactory extends AbstractCoreKeyRangeFactory {

    private final Schema schema;

    public ClassicRangeFactory(final Schema schema) {
        this.schema = schema;
    }

    @Override
    protected List<Range> getRange(final Object vertex, final GraphFilters operation,
                                   final boolean includeEdgesParam) throws RangeFactoryException {
        final boolean includeEdges;
        final boolean includeEntities;
        final boolean seedEqual = operation instanceof SeedMatching
                && SeedMatchingType.EQUAL.equals(((SeedMatching) operation).getSeedMatching());
        if (seedEqual) {
            includeEdges = false;
            includeEntities = true;
        } else {
            includeEdges = includeEdgesParam;
            includeEntities = operation.getView().hasEntities();
        }

        byte[] serialisedVertex;
        try {
            serialisedVertex = ByteArrayEscapeUtils.escape(((ToBytesSerialiser) schema.getVertexSerialiser()).serialise(vertex));
        } catch (final SerialisationException e) {
            throw new RangeFactoryException("Failed to serialise identifier", e);
        }
        if (!includeEntities && !includeEdges) {
            throw new IllegalArgumentException(
                    "Need to include either Entities or Edges or both when getting Range from a type and value");
        }
        if (includeEntities && includeEdges) {
            return Collections.singletonList(getRange(serialisedVertex));
        } else if (includeEntities) {
            return Collections.singletonList(getEntityRangeFromVertex(serialisedVertex));
        } else {
            return Collections.singletonList(getEdgeRangeFromVertex(serialisedVertex));
        }
    }


    @Override
    protected List<Range> getRange(final Object sourceVal, final Object destVal, final DirectedType directed,
                                   final GraphFilters operation, final IncludeIncomingOutgoingType inOutType) throws RangeFactoryException {
        return Collections.singletonList(new Range(getKeyFromEdgeId(sourceVal, destVal, directed, inOutType, false), true,
                getKeyFromEdgeId(sourceVal, destVal, directed, inOutType, true), true));
    }

    protected Key getKeyFromEdgeId(final Object sourceVal, final Object destVal, final DirectedType directed,
                                   final IncludeIncomingOutgoingType inOutType,
                                   final boolean endKey) throws RangeFactoryException {
        final byte directionFlag1;
        if (DirectedType.isEither(directed)) {
            // Get directed and undirected edges
            directionFlag1 = endKey
                    ? ClassicBytePositions.INCORRECT_WAY_DIRECTED_EDGE
                    : ClassicBytePositions.UNDIRECTED_EDGE;
        } else if (directed.isDirected()) {
            if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                directionFlag1 = ClassicBytePositions.INCORRECT_WAY_DIRECTED_EDGE;
            } else {
                directionFlag1 = ClassicBytePositions.CORRECT_WAY_DIRECTED_EDGE;
            }
        } else {
            directionFlag1 = ClassicBytePositions.UNDIRECTED_EDGE;
        }

        final ToBytesSerialiser vertexSerialiser = (ToBytesSerialiser) schema.getVertexSerialiser();

        // Serialise source and destination to byte arrays, escaping if
        // necessary
        byte[] source;
        try {
            source = ByteArrayEscapeUtils.escape(vertexSerialiser.serialise(sourceVal));
        } catch (final SerialisationException e) {
            throw new RangeFactoryException("Failed to serialise Edge Source", e);
        }

        byte[] destination;
        try {
            destination = ByteArrayEscapeUtils.escape(vertexSerialiser.serialise(destVal));
        } catch (final SerialisationException e) {
            throw new RangeFactoryException("Failed to serialise Edge Destination", e);
        }

        // Length of row key is the length of the source
        // plus the length of the destination
        // plus one for the delimiter in between the source and destination
        // plus one for the delimiter in between the destination and the direction flag
        // plus one for the direction flag at the end.
        byte[] key;
        if (endKey) {
            key = new byte[source.length + destination.length + 4];
            key[key.length - 3] = ByteArrayEscapeUtils.DELIMITER;
            key[key.length - 2] = directionFlag1;
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = new byte[source.length + destination.length + 3];
            key[key.length - 2] = ByteArrayEscapeUtils.DELIMITER;
            key[key.length - 1] = directionFlag1;
        }
        // Create first key: source DELIMITER destination DELIMITER (CORRECT_WAY_DIRECTED_EDGE or UNDIRECTED_EDGE)
        // Here if we desire an EdgeID we and the user has asked for incoming
        // edges only for related items, we put the destination first so we find the flipped edge's key,
        // this key will pass the filter iterators check for incoming edges, but
        // the result will be flipped back to the correct edge on the client end conversion
        // Simply put when looking up an EDGE ID that ID counts as both incoming
        // and outgoing so we use the reversed key when looking up incoming.
        byte[] firstValue;
        byte[] secondValue;
        if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
            firstValue = destination;
            secondValue = source;
        } else {
            firstValue = source;
            secondValue = destination;
        }
        System.arraycopy(firstValue, 0, key, 0, firstValue.length);
        key[firstValue.length] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(secondValue, 0, key, firstValue.length + 1, secondValue.length);
        return new Key(key, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
    }

    private Range getRange(final byte[] serialisedVertex) {
        final byte[] endRowKey = new byte[serialisedVertex.length + 1];
        System.arraycopy(serialisedVertex, 0, endRowKey, 0, serialisedVertex.length);
        endRowKey[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        final Key startKey = new Key(serialisedVertex, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES,
                AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
        final Key endKey = new Key(endRowKey, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES,
                Long.MAX_VALUE);
        return new Range(startKey, true, endKey, false);
    }

    private Range getEntityRangeFromVertex(final byte[] serialisedVertex) {
        final byte[] key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 1);
        key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER;
        final Key startKey = new Key(serialisedVertex, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES,
                AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
        final Key endKey = new Key(key, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES,
                Long.MAX_VALUE);
        return new Range(startKey, true, endKey, false);
    }

    private Range getEdgeRangeFromVertex(final byte[] serialisedVertex) {
        final byte[] startRowKey = new byte[serialisedVertex.length + 1];
        System.arraycopy(serialisedVertex, 0, startRowKey, 0, serialisedVertex.length);
        startRowKey[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        // Add delimiter to ensure that we don't get Entities.
        final byte[] endRowKey = new byte[serialisedVertex.length + 1];
        System.arraycopy(serialisedVertex, 0, endRowKey, 0, serialisedVertex.length);
        endRowKey[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        final Key startKey = new Key(startRowKey, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES,
                Long.MAX_VALUE);
        final Key endKey = new Key(endRowKey, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES,
                Long.MAX_VALUE);
        return new Range(startKey, true, endKey, false);
    }
}
