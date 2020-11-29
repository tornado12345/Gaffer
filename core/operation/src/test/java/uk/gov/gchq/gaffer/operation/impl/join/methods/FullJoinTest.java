/*
 * Copyright 2018-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.join.methods;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.operation.impl.join.JoinFunctionTest;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FullJoinTest extends JoinFunctionTest {

    @Override
    protected List<MapTuple> getExpectedLeftKeyResultsForElementMatch() {
        return Arrays.asList(
                createMapTuple(getElement(1), Collections.singletonList(getElement(1))),
                createMapTuple(getElement(2), Lists.newArrayList(getElement(2), getElement(2))),
                createMapTuple(getElement(3), Collections.singletonList(getElement(3))),
                createMapTuple(getElement(3), Collections.singletonList(getElement(3))),
                createMapTuple(getElement(4), Collections.singletonList(getElement(4))),
                createMapTuple(getElement(8), Collections.emptyList()),
                createMapTuple(getElement(10), Collections.emptyList())
        );
    }

    @Override
    protected List<MapTuple> getExpectedRightKeyResultsForElementMatch() {
        return Arrays.asList(
                createMapTuple(Collections.singletonList(getElement(1)), getElement(1)),
                createMapTuple(Collections.singletonList(getElement(2)), getElement(2)),
                createMapTuple(Collections.singletonList(getElement(2)), getElement(2)),
                createMapTuple(Lists.newArrayList(getElement(3), getElement(3)), getElement(3)),
                createMapTuple(Collections.singletonList(getElement(4)), getElement(4)),
                createMapTuple(Collections.emptyList(), getElement(6)),
                createMapTuple(Collections.emptyList(), getElement(12))
        );
    }

    @Override
    protected List<MapTuple> getExpectedLeftKeyResultsFlattenedForElementMatch() {
        return Arrays.asList(
                createMapTuple(getElement(1), getElement(1)),
                createMapTuple(getElement(2), getElement(2)),
                createMapTuple(getElement(2), getElement(2)),
                createMapTuple(getElement(3), getElement(3)),
                createMapTuple(getElement(3), getElement(3)),
                createMapTuple(getElement(4), getElement(4)),
                createMapTuple(getElement(8), null),
                createMapTuple(getElement(10), null)
        );
    }

    @Override
    protected List<MapTuple> getExpectedRightKeyResultsFlattenedForElementMatch() {
        return Arrays.asList(
                createMapTuple(getElement(1), getElement(1)),
                createMapTuple(getElement(2), getElement(2)),
                createMapTuple(getElement(2), getElement(2)),
                createMapTuple(getElement(3), getElement(3)),
                createMapTuple(getElement(3), getElement(3)),
                createMapTuple(getElement(4), getElement(4)),
                createMapTuple(null, getElement(6)),
                createMapTuple(null, getElement(12))
        );
    }

    @Override
    protected List<MapTuple> getExpectedLeftKeyResultsForCustomMatch() {
        return Arrays.asList(
                createMapTuple(getElement(1), Arrays.asList(getElement(2), getElement(2))),
                createMapTuple(getElement(2), Collections.singletonList(getElement(4))),
                createMapTuple(getElement(3), Collections.singletonList(getElement(6))),
                createMapTuple(getElement(3), Collections.singletonList(getElement(6))),
                createMapTuple(getElement(4), Collections.emptyList()),
                createMapTuple(getElement(8), Collections.emptyList()),
                createMapTuple(getElement(10), Collections.emptyList())
        );
    }

    @Override
    protected List<MapTuple> getExpectedRightKeyResultsForCustomMatch() {
        return Arrays.asList(
                createMapTuple(Collections.singletonList(getElement(2)), getElement(1)),
                createMapTuple(Collections.singletonList(getElement(4)), getElement(2)),
                createMapTuple(Collections.singletonList(getElement(4)), getElement(2)),
                createMapTuple(Collections.emptyList(), getElement(3)),
                createMapTuple(Collections.singletonList(getElement(8)), getElement(4)),
                createMapTuple(Collections.emptyList(), getElement(6)),
                createMapTuple(Collections.emptyList(), getElement(12))
        );
    }

    @Override
    protected List<MapTuple> getExpectedLeftKeyResultsFlattenedForCustomMatch() {
        return Arrays.asList(
                createMapTuple(getElement(1), getElement(2)),
                createMapTuple(getElement(1), getElement(2)),
                createMapTuple(getElement(2), getElement(4)),
                createMapTuple(getElement(3), getElement(6)),
                createMapTuple(getElement(3), getElement(6)),
                createMapTuple(getElement(4), null),
                createMapTuple(getElement(8), null),
                createMapTuple(getElement(10), null)
        );
    }

    @Override
    protected List<MapTuple> getExpectedRightKeyResultsFlattenedForCustomMatch() {
        return Arrays.asList(
                createMapTuple(getElement(2), getElement(1)),
                createMapTuple(getElement(4), getElement(2)),
                createMapTuple(getElement(4), getElement(2)),
                createMapTuple(null, getElement(3)),
                createMapTuple(getElement(8), getElement(4)),
                createMapTuple(null, getElement(6)),
                createMapTuple(null, getElement(12))
        );
    }

    @Override
    protected JoinFunction getJoinFunction() {
        return new FullJoin();
    }
}
