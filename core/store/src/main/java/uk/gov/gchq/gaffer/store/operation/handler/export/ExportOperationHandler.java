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

package uk.gov.gchq.gaffer.store.operation.handler.export;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.Export;
import uk.gov.gchq.gaffer.operation.export.Exporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * Abstract class describing how to handle {@link Export} operations.
 *
 * @param <EXPORT> the {@link Export} operation
 * @param <EXPORTER> the {@link Exporter} instance
 */
public abstract class ExportOperationHandler<EXPORT extends Export & Operation, EXPORTER extends Exporter> implements OperationHandler<EXPORT> {
    @Override
    public Object doOperation(final EXPORT export,
                              final Context context, final Store store)
            throws OperationException {
        EXPORTER exporter = context.getExporter(getExporterClass());
        if (null == exporter) {
            exporter = createExporter(export, context, store);
            if (null == exporter) {
                throw new OperationException("Unable to create exporter: " + getExporterClass());
            }
            context.addExporter(exporter);
        }

        return doOperation(export, context, store, exporter);
    }

    protected abstract Class<EXPORTER> getExporterClass();

    protected abstract EXPORTER createExporter(final EXPORT export, final Context context, final Store store);

    protected abstract Object doOperation(final EXPORT export, final Context context, final Store store, final EXPORTER exporter) throws OperationException;
}
