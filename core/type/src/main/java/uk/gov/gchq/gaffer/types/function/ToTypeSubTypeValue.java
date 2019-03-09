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
package uk.gov.gchq.gaffer.types.function;

import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

/**
 * A {@code ToTypeSubTypeValue} is a {@link KorypheFunction} that converts a
 * value into a {@link TypeSubTypeValue}, by setting the Type and SubType to null
 * and the Value to the input value.
 */
@Since("1.8.0")
@Summary("Converts a value into a TypeSubTypeValue")
public class ToTypeSubTypeValue extends KorypheFunction<Object, TypeSubTypeValue> {
    @Override
    public TypeSubTypeValue apply(final Object value) {
        return new TypeSubTypeValue(null, null, null != value ? value.toString() : null);
    }
}
