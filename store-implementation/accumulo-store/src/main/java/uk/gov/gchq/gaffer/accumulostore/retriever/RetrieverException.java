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

package uk.gov.gchq.gaffer.accumulostore.retriever;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;

import static uk.gov.gchq.gaffer.core.exception.Status.INTERNAL_SERVER_ERROR;

public class RetrieverException extends GafferCheckedException {

    private static final long serialVersionUID = -5471324536508286444L;

    public RetrieverException(final String message, final Throwable e) {
        super(message, e, INTERNAL_SERVER_ERROR);
    }

    public RetrieverException(final Throwable e) {
        super(e, INTERNAL_SERVER_ERROR);
    }
}
