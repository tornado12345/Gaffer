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

package uk.gov.gchq.gaffer.rest.service.v2;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.rest.RestApiTestClient;
import uk.gov.gchq.gaffer.rest.SystemStatus;
import uk.gov.gchq.gaffer.rest.application.ApplicationConfigV2;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import java.io.IOException;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

public class RestApiV2TestClient extends RestApiTestClient {

    public RestApiV2TestClient() {
        super("http://localhost:8080/", "rest/", "v2", new ApplicationConfigV2());
    }

    @Override
    public Response executeOperation(final Operation operation) throws IOException {
        startServer();
        return client.target(uriString)
                .path("/graph/operations/execute")
                .request()
                .post(Entity.entity(JSONSerialiser.serialise(operation), APPLICATION_JSON_TYPE));
    }

    @Override
    public Response executeOperationChain(final OperationChain opChain) throws IOException {
        startServer();
        return client.target(uriString)
                .path("/graph/operations/execute")
                .request()
                .post(Entity.entity(JSONSerialiser.serialise(opChain), APPLICATION_JSON_TYPE));
    }

    @Override
    public Response executeOperationChainChunked(final OperationChain opChain) throws IOException {
        return executeOperationChunked(opChain);
    }

    public Response executeOperationChainChunkedWithHeaders(final OperationChain opChain, final String authHeaderVal) throws IOException {
        startServer();
        return client.target(uriString)
                .path("/graph/operations/execute/chunked")
                .request()
                .header("Authorization", authHeaderVal)
                .post(Entity.entity(JSONSerialiser.serialise(opChain), APPLICATION_JSON_TYPE));
    }

    @Override
    public Response executeOperationChunked(final Operation operation) throws IOException {
        startServer();
        return client.target(uriString)
                .path("/graph/operations/execute/chunked")
                .request()
                .post(Entity.entity(JSONSerialiser.serialise(operation), APPLICATION_JSON_TYPE));
    }

    @Override
    public SystemStatus getRestServiceStatus() {
        return client.target(uriString)
                .path("/graph/status")
                .request()
                .get(SystemStatus.class);
    }

    @Override
    public Response getOperationDetails(final Class clazz) throws IOException {
        return client.target(uriString)
                .path("graph/operations/" + clazz.getCanonicalName())
                .request()
                .get(Response.class);
    }

    public Response getAllOperationsAsOperationDetails() throws IOException {
        return client.target(uriString)
                .path("graph/operations/details")
                .request()
                .get(Response.class);
    }

    public Response getProperties() {
        return client.target(uriString)
                .path("/properties")
                .request()
                .get(Response.class);
    }

    public Response getProperty(String property) {
        return client.target(uriString)
                .path("/properties/" + property)
                .request()
                .get(Response.class);
    }
}
