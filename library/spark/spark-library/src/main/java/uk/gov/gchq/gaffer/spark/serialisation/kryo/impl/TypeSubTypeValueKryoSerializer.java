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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import uk.gov.gchq.gaffer.types.TypeSubTypeValue;

/**
 * A {@code TypeSubTypeValueKryoSerializer} is a {@link Kryo} {@link com.esotericsoftware.kryo.Serializer} for
 * a {@link TypeSubTypeValue}
 */
public class TypeSubTypeValueKryoSerializer extends Serializer<TypeSubTypeValue> {
    @Override
    public void write(final Kryo kryo, final Output output, final TypeSubTypeValue typeSubTypeValue) {
        output.writeString(typeSubTypeValue.getType());
        output.writeString(typeSubTypeValue.getSubType());
        output.writeString(typeSubTypeValue.getValue());
    }

    @Override
    public TypeSubTypeValue read(final Kryo kryo, final Input input, final Class<TypeSubTypeValue> aClass) {
        final String type = input.readString();
        final String subType = input.readString();
        final String value = input.readString();
        return new TypeSubTypeValue(type, subType, value);
    }
}
