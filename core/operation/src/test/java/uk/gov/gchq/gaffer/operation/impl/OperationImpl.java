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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.CustomVertex;

import java.util.Date;
import java.util.Map;

@JsonPropertyOrder(alphabetic = true)
public class OperationImpl implements Operation {
    @Required
    private String requiredField1;

    @Required
    // Public so the validation of the required field can be tested differently
    public CustomVertex requiredField2;

    private Date optionalField1;

    private CustomVertex optionalField2;
    private Map<String, String> options;

    public String getRequiredField1() {
        return requiredField1;
    }

    public void setRequiredField1(final String requiredField1) {
        this.requiredField1 = requiredField1;
    }

    public CustomVertex getRequiredField2() {
        return requiredField2;
    }

    public void setRequiredField2(final CustomVertex requiredField2) {
        this.requiredField2 = requiredField2;
    }

    public Date getOptionalField1() {
        return optionalField1;
    }

    public void setOptionalField1(final Date optionalField1) {
        this.optionalField1 = optionalField1;
    }

    public CustomVertex getOptionalField2() {
        return optionalField2;
    }

    public void setOptionalField2(final CustomVertex optionalField2) {
        this.optionalField2 = optionalField2;
    }

    @Override
    public OperationImpl shallowClone() {
        return new OperationImpl.Builder()
                .requiredField1(requiredField1)
                .requiredField2(requiredField2)
                .optionalField1(optionalField1)
                .optionalField2(optionalField2)
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

    public static final class Builder
            extends Operation.BaseBuilder<OperationImpl, OperationImpl.Builder> {
        public Builder() {
            super(new OperationImpl());
        }

        public Builder requiredField1(final String requiredField1) {
            _getOp().setRequiredField1(requiredField1);
            return _self();
        }

        public Builder requiredField2(final CustomVertex requiredField2) {
            _getOp().setRequiredField2(requiredField2);
            return _self();
        }

        public Builder optionalField1(final Date optionalField1) {
            _getOp().setOptionalField1(optionalField1);
            return _self();
        }

        public Builder optionalField2(final CustomVertex optionalField2) {
            _getOp().setOptionalField2(optionalField2);
            return _self();
        }

    }
}
