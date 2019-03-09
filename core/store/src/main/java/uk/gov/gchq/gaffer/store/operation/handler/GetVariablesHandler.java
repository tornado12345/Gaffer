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

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.GetVariables;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetVariablesHandler implements OperationHandler<GetVariables> {
    @Override
    public Map<String, Object> doOperation(final GetVariables operation, final Context context, final Store store) throws OperationException {
        final Map<String, Object> variableMap = new HashMap<>();
        List<String> variableNames = operation.getVariableNames() != null ? operation.getVariableNames() : Lists.newArrayList();
        for (final String key : variableNames) {
            if (null != key) {
                variableMap.put(key, context.getVariable(key));
            }
        }
        return variableMap;
    }
}
