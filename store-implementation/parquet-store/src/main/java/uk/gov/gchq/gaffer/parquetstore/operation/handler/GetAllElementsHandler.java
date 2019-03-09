/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.handler;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.ParquetElementRetriever;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;

/**
 * An {@link OutputOperationHandler} for the {@link GetAllElements} operation on the {@link ParquetStore}.
 */
public class GetAllElementsHandler implements OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> {

    @Override
    public CloseableIterable<? extends Element> doOperation(final GetAllElements operation,
                                                            final Context context,
                                                            final Store store) {
        return doOperation(operation, (ParquetStore) store, context.getUser());
    }

    private CloseableIterable<Element> doOperation(final GetAllElements operation,
                                                   final ParquetStore store,
                                                   final User user) {
        return new ParquetElementRetriever(store, operation, user);
    }
}
