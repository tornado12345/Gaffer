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
package uk.gov.gchq.gaffer.data.element.function;

import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

/**
 * An {@code UnwrapVertex} is a {@link KorypheFunction} for unwrapping {@link EntityId}s.
 * If the input is an EntityId the vertex is extracted, otherwise the original
 * value is returned.
 */
@Since("1.5.0")
@Summary("Extracts the the vertex from an entityId")
public class UnwrapEntityId extends KorypheFunction<Object, Object> {
    @Override
    public Object apply(final Object item) {
        return null != item ? item instanceof EntityId ? ((EntityId) item).getVertex() : item : null;
    }
}
