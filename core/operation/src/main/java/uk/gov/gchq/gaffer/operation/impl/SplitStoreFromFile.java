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
package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * The {@code SplitStoreFromFile} operation is for splitting a store
 * based on a file of split points.
 * The default handler assumes that each line in the files relates to a String split.
 * The default handler will assume the file is available in the local file system,
 * different stores may also attempt to load the file from other file systems like
 * hdfs - this is the case for the Accumulo Store.
 *
 * @see SplitStoreFromFile.Builder
 */
@JsonPropertyOrder(value = {"class", "inputPath"}, alphabetic = true)
@Since("1.1.1")
@Summary("Splits a store based on a file of split points")
public class SplitStoreFromFile implements Operation {
    @Required
    private String inputPath;
    private Map<String, String> options;

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(final String inputPath) {
        this.inputPath = inputPath;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public SplitStoreFromFile shallowClone() {
        return new SplitStoreFromFile.Builder()
                .inputPath(inputPath)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<SplitStoreFromFile, Builder> {
        public Builder() {
            super(new SplitStoreFromFile());
        }

        public Builder inputPath(final String inputPath) {
            _getOp().setInputPath(inputPath);
            return _self();
        }
    }
}
