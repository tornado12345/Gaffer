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

package uk.gov.gchq.gaffer.operation;

/**
 * A {@code SeedMatching} adds seed matching to operations.
 */
public interface SeedMatching {
    /**
     * @param seedMatching a {@link SeedMatchingType} describing how the seeds should be
     *                     matched to the identifiers in the graph.
     * @see SeedMatchingType
     */
    void setSeedMatching(final SeedMatchingType seedMatching);

    SeedMatchingType getSeedMatching();

    /**
     * A {@code SeedMatchingType} defines how the seeds in the operation should be matched.
     */
    enum SeedMatchingType {
        EQUAL, RELATED
    }

    interface Builder<OP extends SeedMatching, B extends Builder<OP, ?>> extends Operation.Builder<OP, B> {
        default B seedMatching(final SeedMatchingType seedMatchingType) {
            _getOp().setSeedMatching(seedMatchingType);
            return _self();
        }
    }
}
