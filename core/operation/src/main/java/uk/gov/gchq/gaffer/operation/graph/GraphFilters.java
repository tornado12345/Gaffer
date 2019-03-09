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

package uk.gov.gchq.gaffer.operation.graph;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;

/**
 * A {@code GraphFilters} is an {@link uk.gov.gchq.gaffer.operation.Operation} which
 * performs additional filtering on the {@link Edge}s returned.
 */
public interface GraphFilters extends OperationView {
    /**
     * @param edge the {@link Edge} to be validated.
     * @return true if the {@link Edge} is valid. Otherwise false and a reason should be logged.
     */
    @Override
    default boolean validate(final Edge edge) {
        return null != edge && validateFlags(edge)
                && validatePreAggregationFilter(edge)
                && validatePostAggregationFilter(edge)
                && validatePostTransformFilter(edge);
    }

    default boolean validateFlags(final Edge edge) {
        final DirectedType dirType = getDirectedType();
        return DirectedType.isEither(dirType)
                || (dirType.isDirected() && edge.isDirected())
                || (dirType.isUndirected() && !edge.isDirected());
    }

    DirectedType getDirectedType();

    void setDirectedType(final DirectedType directedType);

    interface Builder<OP extends GraphFilters, B extends Builder<OP, ?>> extends
            OperationView.Builder<OP, B> {

        default B directedType(final DirectedType directedType) {
            _getOp().setDirectedType(directedType);
            return _self();
        }
    }
}
