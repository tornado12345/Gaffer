/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.mapstore.impl;

import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.operation.CountAllElementsDefaultView;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

/**
 * An {@link uk.gov.gchq.gaffer.store.operation.handler.OperationHandler} for the
 * {@link CountAllElementsDefaultView} operation on the {@link MapStore}.
 */
public class CountAllElementsDefaultViewHandler implements OutputOperationHandler<CountAllElementsDefaultView, Long> {
    @Override
    public Long doOperation(final CountAllElementsDefaultView operation, final Context context, final Store store)
            throws OperationException {
        return doOperation((MapStore) store);
    }

    private Long doOperation(final MapStore mapStore) {
        return mapStore.getMapImpl().countAggElements() + mapStore.getMapImpl().countNonAggElements();
    }
}
