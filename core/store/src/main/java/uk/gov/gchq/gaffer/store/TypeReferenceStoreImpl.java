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

package uk.gov.gchq.gaffer.store;

import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.operation.Operation;

import java.util.Set;

/**
 * {@link TypeReference} implementations for use by the {@link Store} class.
 */
public final class TypeReferenceStoreImpl {
    public static class Schema extends TypeReference<uk.gov.gchq.gaffer.store.schema.Schema> {
    }

    public static class StoreTraits extends TypeReference<Set<StoreTrait>> {
    }

    public static class Operations extends TypeReference<Set<Class<? extends Operation>>> {
    }

    private TypeReferenceStoreImpl() {
        // Private constructor to prevent instantiation.
    }
}
