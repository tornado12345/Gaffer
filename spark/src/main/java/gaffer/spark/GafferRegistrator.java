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

package gaffer.spark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.Properties;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * A custom {@link KryoRegistrator} that serializes Gaffer {@link Element}s.
 */
public class GafferRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(final Kryo kryo) {
        kryo.register(Element.class, new KryoWritableSerializer());
        kryo.register(Entity.class, new KryoWritableSerializer());
        kryo.register(Edge.class, new KryoWritableSerializer());
    }

    public static class KryoWritableSerializer extends Serializer<Element> {

        @Override
        public void write(final Kryo kryo, final Output output, final Element element) {
            output.writeString(element.getGroup());
            kryo.writeClass(output, element.getClass());
            kryo.writeObjectOrNull(output, element.getProperties(), Properties.class);

            if (element instanceof Entity) {
                final Entity e = (Entity) element;
                kryo.writeClass(output, e.getVertex().getClass());
                kryo.writeObjectOrNull(output, e.getVertex(), e.getVertex().getClass());
            } else if (element instanceof Edge) {
                final Edge e = (Edge) element;
                kryo.writeClass(output, e.getSource().getClass());
                kryo.writeObjectOrNull(output, e.getSource(), e.getSource().getClass());
                kryo.writeClass(output, e.getDestination().getClass());
                kryo.writeObjectOrNull(output, e.getDestination(), e.getDestination().getClass());
                kryo.writeObjectOrNull(output, e.isDirected(), Boolean.class);
            }
        }

        @Override
        public Element read(final Kryo kryo, final Input input, final Class<Element> type) {
            final String group = input.readString();
            Registration reg = kryo.readClass(input);
            final Properties props = kryo.readObjectOrNull(input, Properties.class);

            if (reg.getType().equals(Entity.class)) {
                reg = kryo.readClass(input);
                Object vertex = kryo.readObjectOrNull(input, reg.getType());
                Entity entity = new Entity(group, vertex);

                if (props != null) {
                    entity.copyProperties(props);
                }
                return entity;
            } else {
                reg = kryo.readClass(input);
                final Object source = kryo.readObjectOrNull(input, reg.getType());
                reg = kryo.readClass(input);
                final Object destination = kryo.readObjectOrNull(input, reg.getType());
                final Edge edge = new Edge(group, source, destination, kryo.readObjectOrNull(input, Boolean.class));

                if (props != null) {
                    edge.copyProperties(props);
                }
                return edge;
            }
        }
    }
}
