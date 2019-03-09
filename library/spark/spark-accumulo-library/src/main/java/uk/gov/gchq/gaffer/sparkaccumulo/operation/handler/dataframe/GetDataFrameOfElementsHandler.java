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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.HashMap;
import java.util.Map;

public class GetDataFrameOfElementsHandler implements OutputOperationHandler<GetDataFrameOfElements, Dataset<Row>> {

    @Override
    public Dataset<Row> doOperation(final GetDataFrameOfElements operation, final Context context,
                                    final Store store) throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    public Dataset<Row> doOperation(final GetDataFrameOfElements operation, final Context context,
                                    final AccumuloStore store) throws OperationException {
        final Map<String, String> operationOptions;
        if (operation.getOptions() != null) {
            operationOptions = operation.getOptions();
        } else {
            operationOptions = new HashMap<>();
        }

        final AccumuloStoreRelation relation = new AccumuloStoreRelation(context,
                operation.getConverters(),
                operation.getView(),
                store,
                operationOptions);
        return relation.sqlContext().baseRelationToDataFrame(relation);
    }

}
