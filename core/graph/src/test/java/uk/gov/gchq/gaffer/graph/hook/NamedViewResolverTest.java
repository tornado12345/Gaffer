/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class NamedViewResolverTest {

    private static final String NAMED_VIEW_NAME = "namedViewName";
    private static final NamedViewCache CACHE = mock(NamedViewCache.class);
    private static final NamedViewResolver RESOLVER = new NamedViewResolver(CACHE);
    private static final NamedView FULL_NAMED_VIEW = new NamedView.Builder()
            .name(NAMED_VIEW_NAME)
            .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                    .preAggregationFilter(new ElementFilter.Builder()
                            .select(TestPropertyNames.PROP_1)
                            .execute(new ExampleFilterFunction())
                            .build())
                    .build())
            .build();

    @Test
    public void shouldResolveNamedView() throws CacheOperationFailedException {
        // Given
        given(CACHE.getNamedView(NAMED_VIEW_NAME)).willReturn(FULL_NAMED_VIEW);

        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .build())
                        .build())
                .build();

        // When
        RESOLVER.preExecute(opChain, new Context(mock(User.class)));
        GetElements getElements = (GetElements) opChain.getOperations().get(0);

        // Then
        assertEquals(FULL_NAMED_VIEW, getElements.getView());
    }

    @Test
    public void shouldResolveNamedViewAndMergeAnotherView() throws CacheOperationFailedException {
        // Given
        given(CACHE.getNamedView(NAMED_VIEW_NAME)).willReturn(FULL_NAMED_VIEW);
        final View viewToMerge = new View.Builder().edge(TestGroups.EDGE).build();

        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .merge(viewToMerge)
                                .build())
                        .build())
                .build();
        final NamedView namedViewMerged = new NamedView.Builder().merge(FULL_NAMED_VIEW).merge(viewToMerge).build();

        // When
        RESOLVER.preExecute(opChain, new Context(mock(User.class)));
        GetElements getElements = (GetElements) opChain.getOperations().get(0);

        // Then
        assertEquals(namedViewMerged, getElements.getView());
    }

    @Test
    public void shouldResolveNamedViewAndMergeAnotherNamedView() throws CacheOperationFailedException {
        // Given
        given(CACHE.getNamedView(NAMED_VIEW_NAME)).willReturn(FULL_NAMED_VIEW);
        final NamedView namedViewToMerge = new NamedView.Builder().name(NAMED_VIEW_NAME + 1).edge(TestGroups.EDGE).build();
        given(CACHE.getNamedView(NAMED_VIEW_NAME + 1)).willReturn(namedViewToMerge);

        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .merge(namedViewToMerge)
                                .build())
                        .build())
                .build();
        final NamedView namedViewMerged = new NamedView.Builder().merge(FULL_NAMED_VIEW).merge(namedViewToMerge).build();

        // When
        RESOLVER.preExecute(opChain, new Context(mock(User.class)));
        GetElements getElements = (GetElements) opChain.getOperations().get(0);

        // Then
        assertEquals(namedViewMerged, getElements.getView());
    }
}
