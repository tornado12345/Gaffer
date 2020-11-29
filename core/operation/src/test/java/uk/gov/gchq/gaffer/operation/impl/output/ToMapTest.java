/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.output;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.MapGenerator;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class ToMapTest extends OperationTest<ToMap> {

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("elementGenerator");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final ToMap toMap = new ToMap.Builder()
                .input(new Entity(TestGroups.ENTITY), new Entity(TestGroups.ENTITY_2))
                .generator(new MapGenerator())
                .build();

        // Then
        assertThat(toMap.getInput(), is(notNullValue()));
        assertThat(toMap.getInput(), iterableWithSize(2));
        assertThat(toMap.getElementGenerator(), is(notNullValue()));
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Entity input = new Entity(TestGroups.ENTITY);
        final MapGenerator generator = new MapGenerator();
        final ToMap toMap = new ToMap.Builder()
                .input(input)
                .generator(generator)
                .build();

        // When
        ToMap clone = toMap.shallowClone();

        // Then
        assertNotSame(toMap, clone);
        assertEquals(Lists.newArrayList(input), clone.getInput());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Iterable.class, outputClass);
    }

    @Override
    protected ToMap getTestObject() {
        return new ToMap();
    }
}
