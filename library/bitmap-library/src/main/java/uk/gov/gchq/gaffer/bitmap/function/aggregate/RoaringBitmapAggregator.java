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
package uk.gov.gchq.gaffer.bitmap.function.aggregate;

import org.roaringbitmap.RoaringBitmap;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * Aggregator for {@link RoaringBitmap} objects.
 * Bitmaps are aggregated using a bitwise OR operation.
 */
@Since("1.0.0")
@Summary("Aggregates RoaringBitmaps")
public class RoaringBitmapAggregator extends KorypheBinaryOperator<RoaringBitmap> {
    @Override
    protected RoaringBitmap _apply(final RoaringBitmap a, final RoaringBitmap b) {
        a.or(b);
        return a;
    }
}
