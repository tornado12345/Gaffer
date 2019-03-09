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

package uk.gov.gchq.gaffer.hdfs.integration.loader;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.JsonMapperGenerator;
import uk.gov.gchq.gaffer.integration.impl.loader.ParameterizedLoaderIT;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.SchemaLoader;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.schema.TestSchema;
import uk.gov.gchq.gaffer.user.User;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AddElementsFromHdfsLoaderIT extends ParameterizedLoaderIT<AddElementsFromHdfs> {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    protected String inputDir1;
    protected String inputDir2;
    protected String inputDir3;
    protected String outputDir;
    protected String failureDir;
    protected String splitsDir;
    protected String splitsFile;
    protected String workingDir;
    protected String stagingDir;

    public AddElementsFromHdfsLoaderIT(final TestSchema schema, final SchemaLoader loader, final Map<String, User> userMap) {
        super(schema, loader, userMap);
    }

    @Override
    public void _setup() throws Exception {
        inputDir1 = testFolder.getRoot().getAbsolutePath() + "/inputDir1";
        inputDir2 = testFolder.getRoot().getAbsolutePath() + "/inputDir2";
        inputDir3 = testFolder.getRoot().getAbsolutePath() + "/inputDir3";

        outputDir = testFolder.getRoot().getAbsolutePath() + "/outputDir";
        failureDir = testFolder.getRoot().getAbsolutePath() + "/failureDir";
        splitsDir = testFolder.getRoot().getAbsolutePath() + "/splitsDir";
        splitsFile = splitsDir + "/splits";
        workingDir = testFolder.getRoot().getAbsolutePath() + "/workingDir";
        stagingDir = testFolder.getRoot().getAbsolutePath() + "/stagingDir";
        super._setup();
    }

    @Test
    public void shouldThrowExceptionWhenAddElementsFromHdfsWhenFailureDirectoryContainsFiles() throws Exception {
        tearDown();
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(failureDir));
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(failureDir + "/someFile.txt"), true)))) {
            writer.write("Some content");
        }

        try {
            setup();
            fail("Exception expected");
        } catch (final OperationException e) {
            assertEquals("Failure directory is not empty: " + failureDir, e.getCause().getMessage());
        } finally {
            tearDown();
        }
    }

    @Test
    public void shouldAddElementsFromHdfsWhenDirectoriesAlreadyExist() throws Exception {
        // Given
        tearDown();
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(failureDir));

        // When
        setup();

        // Then
        shouldGetAllElements();
    }

    @Test
    public void shouldThrowExceptionWhenAddElementsFromHdfsWhenOutputDirectoryContainsFiles() throws Exception {
        // Given
        tearDown();
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(outputDir));
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputDir + "/someFile.txt"), true)))) {
            writer.write("Some content");
        }

        // When
        try {
            setup();
            fail("Exception expected");
        } catch (final Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Output directory exists and is not empty: " + outputDir));
        } finally {
            tearDown();
        }
    }

    @Override
    protected void addElements(final Iterable<? extends Element> elements) throws OperationException {
        createInputFile(elements);
        graph.execute(new AddElementsFromHdfs.Builder()
                .addInputMapperPair(new Path(inputDir1).toString(), JsonMapperGenerator.class)
                .addInputMapperPair(new Path(inputDir2).toString(), JsonMapperGenerator.class)
                .addInputMapperPair(new Path(inputDir3).toString(), JsonMapperGenerator.class)
                .outputPath(outputDir)
                .failurePath(failureDir)
                .jobInitialiser(new TextJobInitialiser())
                .useProvidedSplits(false)
                .splitsFilePath(splitsFile)
                .workingPath(workingDir)
                .build(), user);
    }

    private void createInputFile(final Iterable<? extends Element> elements) {
        final Path inputPath1 = new Path(inputDir1);
        final Path inputFilePath1 = new Path(inputDir1 + "/file.txt");
        final Path inputPath2 = new Path(inputDir2);
        final Path inputFilePath2 = new Path(inputDir2 + "/file.txt");
        final Path inputPath3 = new Path(inputDir3);
        final Path inputFilePath3 = new Path(inputDir3 + "/file.txt");
        try {
            final FileSystem fs = FileSystem.getLocal(createLocalConf());
            fs.mkdirs(inputPath1);
            fs.mkdirs(inputPath2);
            fs.mkdirs(inputPath3);

            if (fs.exists(inputFilePath1)) {
                fs.delete(inputFilePath1, false);
            }
            if (fs.exists(inputFilePath2)) {
                fs.delete(inputFilePath2, false);
            }
            if (fs.exists(inputFilePath3)) {
                fs.delete(inputFilePath3, false);
            }

            try (final BufferedWriter writer1 = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath1, true)));
                 final BufferedWriter writer2 = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath2, true)));
                 final BufferedWriter writer3 = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath3, true)))) {
                final Random rand = new Random();
                for (final Element element : elements) {
                    final int randVal = rand.nextInt(3);
                    final BufferedWriter writer;
                    if (randVal == 0) {
                        writer = writer1;
                    } else if (randVal == 1) {
                        writer = writer2;
                    } else {
                        writer = writer3;
                    }
                    writer.write(StringUtil.toString(JSONSerialiser.serialise(element)) + "\n");
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException("Unable to create input files", e);
        }
    }

    private JobConf createLocalConf() {
        // Set up local conf
        final JobConf conf = new JobConf();
        conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
        conf.set(JTConfig.JT_IPC_ADDRESS, JTConfig.LOCAL_FRAMEWORK_NAME);

        return conf;
    }
}
