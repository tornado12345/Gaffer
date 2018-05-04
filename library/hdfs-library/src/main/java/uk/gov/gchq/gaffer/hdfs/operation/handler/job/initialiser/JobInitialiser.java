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
package uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser;

import org.apache.hadoop.mapreduce.Job;

import uk.gov.gchq.gaffer.hdfs.operation.MapReduce;
import uk.gov.gchq.gaffer.store.Store;

import java.io.IOException;

/**
 * A {@code JobInitialiser} initialises a job.
 *
 * @see AvroJobInitialiser
 * @see TextJobInitialiser
 */
public interface JobInitialiser {
    /**
     * Initialises a job. This will probably involve setting up the job configuration.
     *
     * @param job       the {@link Job} to be initialised
     * @param operation the {@link MapReduce} containing configuration.
     * @param store     the {@link Store} that will handle the {@link uk.gov.gchq.gaffer.operation.Operation}
     * @throws IOException if IO issues occur
     */
    void initialiseJob(final Job job, final MapReduce operation, final Store store) throws IOException;
}
