/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphReadAccessPredicate;
import uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphWriteAccessPredicate;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.function.CallMethod;
import uk.gov.gchq.koryphe.impl.predicate.CollectionContains;
import uk.gov.gchq.koryphe.predicate.AdaptedPredicate;

import java.util.Collections;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.user.StoreUser.ALL_USERS;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedAccessAuthTest {

    private static final String AUTH_X = "X";
    private static final User TEST_USER = testUser();

    @Test
    public void shouldValidateUserWithMatchingAuth() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths(ALL_USERS)
                .build();

        assertTrue(access.hasReadAccess(TEST_USER));
    }

    @Test
    public void shouldValidateUserWithSubsetAuth() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths(ALL_USERS, AUTH_X)
                .build();

        assertTrue(access.hasReadAccess(TEST_USER));
    }

    @Test
    public void shouldValidateUserWithSurplusMatchingAuth() throws Exception {
        final User user = authUser();

        assertTrue(user.getOpAuths().contains(AUTH_1));

        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths(ALL_USERS)
                .build();

        assertTrue(access.hasReadAccess(user));
    }

    @Test
    public void shouldInValidateUserWithNoAuth() throws Exception {

        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths(ALL_USERS)
                .build();

        assertFalse(access.hasReadAccess(blankUser()));
    }

    @Test
    public void shouldInValidateUserWithMismatchedAuth() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths("X")
                .build();

        assertFalse(access.hasReadAccess(TEST_USER));
    }

    @Test
    public void shouldValidateWithListOfAuths() throws Exception {

        final FederatedAccess access = new FederatedAccess.Builder()
                .addGraphAuths(asList(AUTH_1))
                .addGraphAuths(asList("X"))
                .build();

        assertTrue(access.hasReadAccess(authUser()));
    }

    @Test
    public void shouldDeserialiseDefaultPredicateIfNotSpecified() throws SerialisationException {
        // Given
        String json = "{" +
                "   \"addingUserId\": \"user1\"," +
                "   \"public\": true," +
                "   \"graphAuths\": [ \"auth1\", \"auth2\" ]" +
                "}";

        // When
        FederatedAccess deserialised = JSONSerialiser.deserialise(json, FederatedAccess.class);

        // Then
        FederatedGraphReadAccessPredicate expectedReadPredicate = new FederatedGraphReadAccessPredicate("user1", Sets.newHashSet("auth1", "auth2"), true);
        FederatedGraphWriteAccessPredicate expectedWritePredicate = new FederatedGraphWriteAccessPredicate("user1");

        assertEquals(expectedReadPredicate, deserialised.getOrDefaultReadAccessPredicate());
        assertEquals(expectedWritePredicate, deserialised.getOrDefaultWriteAccessPredicate());
    }

    @Test
    public void shouldSerialiseAndDeserialiseAccessPredicatesToJson() throws SerialisationException {
        // Given
        final FederatedAccess federatedAccess = new FederatedAccess.Builder()
                .addingUserId("blah")
                .isPublic(false)
                .readAccessPredicate(new AccessPredicate(new AdaptedPredicate(new CallMethod("getDataAuths"), new CollectionContains("specialData"))))
                .writeAccessPredicate(new AccessPredicate(new AdaptedPredicate(new CallMethod("getDataAuths"), new CollectionContains("specialWriteAuth"))))
                .build();

        // When
        final String serialised = new String(JSONSerialiser.serialise(federatedAccess));

        // Then
        final String expected = "{" +
                "   \"addingUserId\": \"blah\"," +
                "   \"public\": false," +
                "   \"disabledByDefault\": false," +
                "   \"readAccessPredicate\": {" +
                "       \"class\": \"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\"," +
                "       \"userPredicate\": {" +
                "           \"class\": \"uk.gov.gchq.koryphe.predicate.AdaptedPredicate\"," +
                "           \"inputAdapter\": {" +
                "               \"class\": \"uk.gov.gchq.koryphe.impl.function.CallMethod\"," +
                "               \"method\": \"getDataAuths\"" +
                "           }," +
                "           \"predicate\": {" +
                "               \"class\": \"uk.gov.gchq.koryphe.impl.predicate.CollectionContains\"," +
                "               \"value\": \"specialData\"" +
                "           }" +
                "       }" +
                "   }," +
                "   \"writeAccessPredicate\": {" +
                "       \"class\": \"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\"," +
                "       \"userPredicate\": {" +
                "           \"class\": \"uk.gov.gchq.koryphe.predicate.AdaptedPredicate\"," +
                "           \"inputAdapter\": {" +
                "               \"class\": \"uk.gov.gchq.koryphe.impl.function.CallMethod\"," +
                "               \"method\": \"getDataAuths\"" +
                "           }," +
                "           \"predicate\": {" +
                "               \"class\": \"uk.gov.gchq.koryphe.impl.predicate.CollectionContains\"," +
                "               \"value\": \"specialWriteAuth\"" +
                "           }" +
                "       }" +
                "   }" +
                "}";

        JsonAssert.assertEquals(expected, serialised);
        final FederatedAccess deserialised = JSONSerialiser.deserialise(serialised, FederatedAccess.class);
        assertEquals(federatedAccess, deserialised);
    }

    @Test
    public void shouldSerialiseAndDeserialiseGraphAuthsToJson() throws SerialisationException {
        // Given
        final FederatedAccess federatedAccess = new FederatedAccess.Builder()
                .addingUserId("blah")
                .isPublic(false)
                .graphAuths("a", "b", "c")
                .build();

        // When
        final String serialised = new String(JSONSerialiser.serialise(federatedAccess));

        // Then
        final String expected = "{" +
                "   \"addingUserId\": \"blah\"," +
                "   \"public\": false," +
                "   \"disabledByDefault\": false," +
                "   \"graphAuths\": [\"a\", \"b\", \"c\"]" +
                "}";

        JsonAssert.assertEquals(expected, serialised);
        final FederatedAccess deserialised = JSONSerialiser.deserialise(serialised, FederatedAccess.class);
        assertEquals(federatedAccess, deserialised);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenBothGraphAuthsAndReadAccessPredicateAreSupplied() {
        final Executable executable = () -> new FederatedAccess.Builder()
                .graphAuths("X")
                .readAccessPredicate(new AccessPredicate("user1", Collections.singletonList("auth1")))
                .build();
        assertThrows(IllegalArgumentException.class, executable, "Only one of graphAuths or readAccessPredicate should be supplied.");
    }
}
