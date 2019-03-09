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
package uk.gov.gchq.gaffer.time.binaryoperator;

import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code RBMBackedTimestampSetAggregator} is a {@link java.util.function.BinaryOperator} that takes in
 * {@link RBMBackedTimestampSet}s and merges the underlying {@code RoaringBitmap}s together.
 */
@Since("1.0.0")
@Summary("Aggregates RBMBackedTimestampSet objects")
public class RBMBackedTimestampSetAggregator extends KorypheBinaryOperator<RBMBackedTimestampSet> {

    @Override
    protected RBMBackedTimestampSet _apply(final RBMBackedTimestampSet a, final RBMBackedTimestampSet b) {
        if (!b.getTimeBucket().equals(a.getTimeBucket())) {
            throw new RuntimeException("Can't aggregate two RBMBackedTimestampSet with different time buckets: "
                    + "a had bucket " + a.getTimeBucket() + ", b had bucket " + b.getTimeBucket());
        }
        a.addAll(b);
        return a;
    }
}
