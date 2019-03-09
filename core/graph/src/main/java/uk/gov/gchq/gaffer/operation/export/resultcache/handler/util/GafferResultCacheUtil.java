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

package uk.gov.gchq.gaffer.operation.export.resultcache.handler.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.predicate.AgeOff;

/**
 * Utility methods for maintaining a Gaffer result cache.
 */
public final class GafferResultCacheUtil {
    public static final long ONE_DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000L;
    public static final long DEFAULT_TIME_TO_LIVE = ONE_DAY_IN_MILLISECONDS;
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferResultCacheUtil.class);

    private GafferResultCacheUtil() {
        // Private constructor to prevent instantiation.
    }

    public static Graph createGraph(final String graphId, final String cacheStorePropertiesPath, final Long timeToLive) {
        if (null == cacheStorePropertiesPath) {
            throw new IllegalArgumentException("Gaffer result cache Store properties are required");
        }

        final Graph.Builder graphBuilder = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .build())
                .storeProperties(cacheStorePropertiesPath)
                .addSchema(createSchema(timeToLive));

        final Graph graph = graphBuilder.build();
        if (!graph.hasTrait(StoreTrait.STORE_VALIDATION)) {
            LOGGER.warn("Gaffer JSON export graph does not have {} trait so results may not be aged off.", StoreTrait.STORE_VALIDATION.name());
        }

        return graph;
    }

    public static Schema createSchema(final Long timeToLive) {
        final Schema.Builder builder = new Schema.Builder()
                .json(StreamUtil.openStreams(GafferResultCacheUtil.class, "gafferResultCache/schema"));

        if (null != timeToLive) {
            builder.merge(new Schema.Builder()
                    .type("timestamp", new TypeDefinition.Builder()
                            .validateFunctions(new AgeOff(timeToLive))
                            .build())
                    .build());
        }

        return builder.build();
    }
}
