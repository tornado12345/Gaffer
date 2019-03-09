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
package uk.gov.gchq.gaffer.hbasestore.integration;

import uk.gov.gchq.gaffer.hbasestore.MiniHBaseStore;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.integration.graph.SchemaHidingIT;
import uk.gov.gchq.gaffer.store.StoreException;

public class HBaseSchemaHidingIT extends SchemaHidingIT {
    public HBaseSchemaHidingIT() {
        super("minihbasestore.properties");
    }

    @Override
    protected void cleanUp() {
        try {
            TableUtils.dropAllTables(new MiniHBaseStore().getConnection());
        } catch (StoreException e) {
            // ignore any errors that occur when dropping test tables
        }
    }
}
