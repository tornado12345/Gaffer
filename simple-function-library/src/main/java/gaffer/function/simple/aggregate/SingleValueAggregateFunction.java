/*
 * Copyright 2016 Crown Copyright
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

package gaffer.function.simple.aggregate;

import gaffer.function.AggregateFunction;
import gaffer.function.SimpleAggregateFunction;
import gaffer.function.annotation.Inputs;
import gaffer.function.annotation.Outputs;

/**
 * An <code>SingleValueAggregateFunction</code> is an {@link SimpleAggregateFunction}
 * that should only ever aggregate the same values together. If different
 * values are passed to the function a {link IllegalArgumentException} is thrown.
 */
@Inputs(Object.class)
@Outputs(Object.class)
public class SingleValueAggregateFunction extends SimpleAggregateFunction<Object> {
    private static final String EXCEPTION_MESSAGE = "This aggregate function does not supported aggregating values together.";

    private Object value;

    @Override
    public void init() {
        value = null;
    }

    @Override
    protected void _aggregate(final Object input) {
        if (null == value) {
            value = input;
        } else if (!value.equals(input)) {
            throw new IllegalArgumentException(EXCEPTION_MESSAGE);
        }
    }

    @Override
    protected Object _state() {
        return value;
    }

    @Override
    public AggregateFunction statelessClone() {
        return new SingleValueAggregateFunction();
    }
}
