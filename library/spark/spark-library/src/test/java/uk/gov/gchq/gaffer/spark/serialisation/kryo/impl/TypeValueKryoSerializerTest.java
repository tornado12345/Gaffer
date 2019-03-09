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
package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl;

import uk.gov.gchq.gaffer.spark.serialisation.kryo.KryoSerializerTest;
import uk.gov.gchq.gaffer.types.TypeValue;

import static org.junit.Assert.assertEquals;

public class TypeValueKryoSerializerTest extends KryoSerializerTest<TypeValue> {

    @Override
    protected void shouldCompareSerialisedAndDeserialisedObjects(final TypeValue obj, final TypeValue deserialised) {
        assertEquals(obj, deserialised);
    }

    @Override
    protected Class<TypeValue> getTestClass() {
        return TypeValue.class;
    }

    @Override
    protected TypeValue getTestObject() {
        return new TypeValue("type", "value");
    }
}
