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
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code ScoreOperationChain} operation determines a "score" for an {@link OperationChain},
 * and is used to determine whether a particular user has the required permissions
 * to execute a given {@link OperationChain}.
 */
@JsonPropertyOrder(value = {"class", "operationChain"}, alphabetic = true)
@Since("1.0.0")
@Summary("Scores an OperationChain")
public class ScoreOperationChain implements Output<Integer> {
    private OperationChain operationChain;
    private Map<String, String> options;

    @Override
    public TypeReference<Integer> getOutputTypeReference() {
        return new TypeReferenceImpl.Integer();
    }

    public OperationChain getOperationChain() {
        return operationChain;
    }

    public void setOperationChain(final OperationChain operationChain) {
        this.operationChain = operationChain;
    }

    @Override
    public ScoreOperationChain shallowClone() {
        return new ScoreOperationChain.Builder()
                .operationChain(operationChain)
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static class Builder extends BaseBuilder<ScoreOperationChain, Builder> implements
            Output.Builder<ScoreOperationChain, Integer, Builder> {
        public Builder() {
            super(new ScoreOperationChain());
        }

        public Builder operationChain(final OperationChain opChain) {
            _getOp().setOperationChain(opChain);
            return _self();
        }
    }
}
