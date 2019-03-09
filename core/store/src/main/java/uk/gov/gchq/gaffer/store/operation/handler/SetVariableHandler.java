/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SetVariable;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

public class SetVariableHandler implements OperationHandler<SetVariable> {
    @Override
    public Void doOperation(final SetVariable operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getVariableName()) {
            throw new IllegalArgumentException("Variable name cannot be null");
        }

        if (null == operation.getInput()) {
            throw new IllegalArgumentException("Variable input value cannot be null");
        }

        context.setVariable(operation.getVariableName(), operation.getInput());
        return null;
    }
}
