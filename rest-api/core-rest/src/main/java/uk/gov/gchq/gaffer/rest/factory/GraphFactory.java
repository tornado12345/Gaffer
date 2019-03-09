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

package uk.gov.gchq.gaffer.rest.factory;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.rest.SystemProperty;

/**
 * A {@code GraphFactory} creates instances of {@link uk.gov.gchq.gaffer.graph.Graph} to be reused for all queries.
 */
public interface GraphFactory {

    static GraphFactory createGraphFactory() {
        final String graphFactoryClass = System.getProperty(SystemProperty.GRAPH_FACTORY_CLASS,
                SystemProperty.GRAPH_FACTORY_CLASS_DEFAULT);

        try {
            return Class.forName(graphFactoryClass)
                    .asSubclass(GraphFactory.class)
                    .newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to create graph factory from class: " + graphFactoryClass, e);
        }
    }

    /**
     * Create a new {@link Graph.Builder} instance.
     *
     * @return the graph builder
     */
    Graph.Builder createGraphBuilder();

    /**
     * Create a new {@link Graph} instance.
     *
     * By default this is created from the {@link Graph.Builder}.
     *
     * @return the graph
     */
    default Graph createGraph() {
        return createGraphBuilder().build();
    }

    /**
     * Get the {@link Graph} instance.
     *
     * @return the graph
     */
    Graph getGraph();
}
