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
package uk.gov.gchq.gaffer.hbasestore.operation.hdfs.handler.job.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.operation.hdfs.mapper.AddElementsFromHdfsMapper;
import uk.gov.gchq.gaffer.hbasestore.operation.hdfs.reducer.AddElementsFromHdfsReducer;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.AddElementsFromHdfsJobFactory;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;

public class HBaseAddElementsFromHdfsJobFactory implements AddElementsFromHdfsJobFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseAddElementsFromHdfsJobFactory.class);

    @Override
    public void prepareStore(final Store store) throws StoreException {
        TableUtils.ensureTableExists(((HBaseStore) store));
    }

    @Override
    public JobConf createJobConf(final AddElementsFromHdfs operation, final String mapperGeneratorClassName, final Store store) throws IOException {
        final JobConf jobConf = new JobConf(((HBaseStore) store).getConfiguration());

        LOGGER.info("Setting up job conf");
        jobConf.set(SCHEMA, new String(store.getSchema().toCompactJson(), CommonConstants.UTF_8));
        LOGGER.info("Added {} {} to job conf", SCHEMA, new String(store.getSchema().toCompactJson(), CommonConstants.UTF_8));
        jobConf.set(MAPPER_GENERATOR, mapperGeneratorClassName);
        LOGGER.info("Added {} of {} to job conf", MAPPER_GENERATOR, mapperGeneratorClassName);
        jobConf.set(VALIDATE, String.valueOf(operation.isValidate()));
        LOGGER.info("Added {} option of {} to job conf", VALIDATE, operation.isValidate());

        Integer numTasks = operation.getNumMapTasks();
        if (null != numTasks) {
            jobConf.setNumMapTasks(numTasks);
            LOGGER.info("Set number of map tasks to {} on job conf", numTasks);
        }

        numTasks = operation.getNumReduceTasks();
        if (null != numTasks) {
            jobConf.setNumReduceTasks(numTasks);
            LOGGER.info("Set number of reduce tasks to {} on job conf", numTasks);
        }
        return jobConf;
    }

    @Override
    public void setupJob(final Job job, final AddElementsFromHdfs operation, final String mapperGeneratorClassName, final Store store) throws IOException {
        job.setJarByClass(getClass());
        job.setJobName(getJobName(mapperGeneratorClassName, operation.getOutputPath()));

        setupMapper(job);
        setupOutput(job, operation, (HBaseStore) store);
        job.setSortComparatorClass(HBaseComparator.class);
        setupReducer(job);
    }

    public static class HBaseComparator implements RawComparator<ImmutableBytesWritable> {
        private static final int LENGTH_BYTES = 4;

        @Override
        public int compare(final byte[] b1, final int s1, final int l1, final byte[] b2, final int s2, final int l2) {
            return KeyValue.COMPARATOR.compare(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES, b2, s2 + LENGTH_BYTES, l2 - LENGTH_BYTES);
        }

        @Override
        public int compare(final ImmutableBytesWritable b1, final ImmutableBytesWritable b2) {
            return KeyValue.COMPARATOR.compare(b1.get(), b1.getOffset(), b1.getLength(), b2.get(), b2.getOffset(), b2.getLength());
        }
    }

    protected String getJobName(final String mapperGenerator, final String outputPath) {
        return "Ingest HDFS data: Generator=" + mapperGenerator + ", output=" + outputPath;
    }

    protected void setupMapper(final Job job) {
        job.setMapperClass(AddElementsFromHdfsMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
    }

    protected void setupReducer(final Job job) {
        job.setReducerClass(AddElementsFromHdfsReducer.class);
    }

    protected void setupOutput(final Job job, final AddElementsFromHdfs operation, final HBaseStore store) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(operation.getOutputPath()));
        String stagingDir = operation.getOption(HBaseStoreConstants.OPERATION_HDFS_STAGING_PATH);
        if (StringUtils.isEmpty(stagingDir)) {
            if (StringUtils.isNotEmpty(operation.getWorkingPath())) {
                stagingDir = operation.getWorkingPath() + (operation.getWorkingPath().endsWith("/") ? "" : "/") + "stagingDir";
            }
        }
        if (StringUtils.isNotEmpty(stagingDir)) {
            job.getConfiguration().set(HConstants.TEMPORARY_FS_DIRECTORY_KEY, stagingDir);
        }

        try {
            HFileOutputFormat2.configureIncrementalLoad(
                    job,
                    store.getTable(),
                    store.getConnection().getRegionLocator(store.getTableName())
            );
        } catch (final StoreException e) {
            throw new RuntimeException(e);
        }
    }
}

