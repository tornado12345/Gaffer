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

package uk.gov.gchq.gaffer.rest.service.accumulo.v2;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.rest.AbstractRestApiIT;
import uk.gov.gchq.gaffer.rest.service.v2.OperationServiceV2IT.OperationDetailPojo;
import uk.gov.gchq.gaffer.rest.service.v2.RestApiV2TestClient;
import uk.gov.gchq.gaffer.store.schema.Schema;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.rest.service.v2.OperationServiceV2IT.OperationFieldPojo;

public class OperationServiceV2IT extends AbstractRestApiIT {

    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(OperationServiceV2IT.class));

    @BeforeEach
    @Override
    public void before() throws IOException {
        client.startServer();
        client.reinitialiseGraph(testFolder, new Schema(), PROPERTIES);
    }

    @Test
    public void shouldReturnOptionsAndSummariesForEnumFields() throws Exception {
        // When
        Response response = client.getOperationDetails(GetElementsBetweenSets.class);

        // Then
        final byte[] json = response.readEntity(byte[].class);
        final OperationDetailPojo opDetails = JSONSerialiser.deserialise(json, OperationDetailPojo.class);
        final Set<OperationFieldPojo> expectedFields = makeExpectedOperationSet();

        assertEquals(expectedFields, Sets.newHashSet(opDetails.getFields()));
    }

    private Set<OperationFieldPojo> makeExpectedOperationSet() {
        return Sets.newHashSet(
                    new OperationFieldPojo("input", "java.lang.Object[]", false, null, null),
                    new OperationFieldPojo("view", "uk.gov.gchq.gaffer.data.elementdefinition.view.View", false, null, null),
                    new OperationFieldPojo("includeIncomingOutGoing", "java.lang.String", false, "Should the edges point towards, or away from your seeds", Sets.newHashSet("INCOMING", "EITHER", "OUTGOING")),
                    new OperationFieldPojo("inputB", "java.lang.Object[]", false, null, null),
                    new OperationFieldPojo("seedMatching", "java.lang.String", false, "How should the seeds be matched?", Sets.newHashSet("RELATED", "EQUAL")),
                    new OperationFieldPojo("options", "java.util.Map<java.lang.String,java.lang.String>", false, null, null),
                    new OperationFieldPojo("directedType", "java.lang.String", false, "Is the Edge directed?", Sets.newHashSet("DIRECTED", "UNDIRECTED", "EITHER")),
                    new OperationFieldPojo("views", "java.util.List<uk.gov.gchq.gaffer.data.elementdefinition.view.View>", false, null, null)
            );
    }

    @Override
    protected RestApiV2TestClient getClient() {
        return new RestApiV2TestClient();
    }
}
