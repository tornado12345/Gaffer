/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;


public class LimitTest extends OperationTest<Limit> {

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("resultLimit");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Limit<String> limit = new Limit.Builder<String>().input("1", "2").resultLimit(1).build();

        // Then
        assertThat(limit.getInput(), is(notNullValue()));
        assertThat(limit.getInput(), iterableWithSize(2));
        assertThat(limit.getResultLimit(), is(1));
        assertThat(limit.getInput(), containsInAnyOrder("1", "2"));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String input = "1";
        final int resultLimit = 4;
        final Limit limit = new Limit.Builder<>()
                .input(input)
                .resultLimit(resultLimit)
                .truncate(false)
                .build();

        // When
        final Limit clone = limit.shallowClone();

        // Then
        assertNotSame(limit, clone);
        assertEquals(input, clone.getInput().iterator().next());
        assertEquals(resultLimit, (int) clone.getResultLimit());
        assertFalse(clone.getTruncate());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Iterable.class, outputClass);
    }

    @Override
    protected Limit getTestObject() {
        return new Limit();
    }
}
