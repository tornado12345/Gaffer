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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.SparkContextUtil;
import uk.gov.gchq.gaffer.spark.operation.dataframe.ClassTagConstants;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd.GetRDDOfElementsInRanges;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

public class GetRDDOfElementsInRangesHandler extends AbstractGetRDDHandler<GetRDDOfElementsInRanges, RDD<Element>> {

    @Override
    public RDD<Element> doOperation(final GetRDDOfElementsInRanges operation,
                                    final Context context,
                                    final Store store) throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    private RDD<Element> doOperation(final GetRDDOfElementsInRanges operation,
                                     final Context context,
                                     final AccumuloStore accumuloStore)
            throws OperationException {
        final Configuration conf = getConfiguration(operation);
        final SparkContext sparkContext = SparkContextUtil.getSparkSession(context, accumuloStore.getProperties()).sparkContext();
        sparkContext.hadoopConfiguration().addResource(conf);
        // Use batch scan option when performing seeded operation
        InputConfigurator.setBatchScan(AccumuloInputFormat.class, conf, true);
        addIterators(accumuloStore, conf, context.getUser(), operation);
        addRangesFromPairs(accumuloStore, conf, operation);
        final RDD<Tuple2<Element, NullWritable>> pairRDD = sparkContext.newAPIHadoopRDD(conf,
                ElementInputFormat.class,
                Element.class,
                NullWritable.class);
        return pairRDD.map(new FirstElement(), ClassTagConstants.ELEMENT_CLASS_TAG);
    }
}
