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

package uk.gov.gchq.gaffer.commonutil.iterable;

import uk.gov.gchq.gaffer.commonutil.stream.FlatMapStreamSupplier;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A {@code StreamFlatMapIterable} is an {@link StreamIterable} which uses a {@link FlatMapStreamSupplier}
 * to combine {@link Iterable}s.
 *
 * @param <I> the type of items in the input iterable
 * @param <O> the type of items in the output stream
 */
public class StreamFlatMapIterable<I, O> extends StreamIterable<O> {
    public StreamFlatMapIterable(final Iterable<I> input, final Function<? super I, ? extends Stream<O>> function) {
        super(new FlatMapStreamSupplier<>(input, function));
    }
}
