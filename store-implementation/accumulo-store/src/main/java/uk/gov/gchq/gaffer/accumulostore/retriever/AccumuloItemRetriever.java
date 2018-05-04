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

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyCloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewUtil;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

public abstract class AccumuloItemRetriever<OP extends Output<CloseableIterable<? extends Element>> & GraphFilters, I_ITEM>
        extends AccumuloRetriever<OP, Element> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloItemRetriever.class);

    protected final boolean includeMatchedVertex;
    private final Iterable<? extends I_ITEM> ids;

    protected AccumuloItemRetriever(final AccumuloStore store, final OP operation,
                                    final User user, final boolean includeMatchedVertex,
                                    final IteratorSetting... iteratorSettings) throws StoreException {
        super(store, operation, user, iteratorSettings);
        this.includeMatchedVertex = includeMatchedVertex;
        this.ids = operation instanceof Input ? ((Input<Iterable<? extends I_ITEM>>) operation).getInput() : null;
    }

    /**
     * Only 1 iterator can be open at a time.
     *
     * @return a closeable iterator of items.
     */
    @Override
    public CloseableIterator<Element> iterator() {
        CloseableUtil.close(iterator);

        final Iterator<? extends I_ITEM> idIterator = null != ids ? ids.iterator() : Iterators.emptyIterator();
        if (!idIterator.hasNext()) {
            return new EmptyCloseableIterator<>();
        }

        try {
            iterator = new ElementIterator(idIterator);
        } catch (final RetrieverException e) {
            LOGGER.error("{} returning empty iterator", e.getMessage(), e);
            return new EmptyCloseableIterator<>();
        }

        return iterator;
    }

    protected abstract void addToRanges(final I_ITEM seed, final Set<Range> ranges) throws RangeFactoryException;

    protected class ElementIterator implements CloseableIterator<Element> {
        private final Iterator<? extends I_ITEM> idsIterator;
        private int count;
        private BatchScanner scanner;
        private Iterator<Entry<Key, Value>> scannerIterator;
        private Element nextElm;

        public ElementIterator(final Iterator<? extends I_ITEM> idIterator) throws RetrieverException {
            idsIterator = idIterator;
            count = 0;
            final Set<Range> ranges = new HashSet<>();
            while (idsIterator.hasNext() && count < store.getProperties().getMaxEntriesForBatchScanner()) {
                count++;
                try {
                    addToRanges(idsIterator.next(), ranges);
                } catch (final RangeFactoryException e) {
                    LOGGER.error("Failed to create a range from given seed pair", e);
                }
            }

            // Create BatchScanner, appropriately configured (i.e. ranges,
            // iterators, etc).
            try {
                scanner = getScanner(ranges);
            } catch (final Exception e) {
                CloseableUtil.close(idsIterator);
                CloseableUtil.close(ids);
                throw new RetrieverException(e);
            }
            scannerIterator = scanner.iterator();
        }

        @Override
        public boolean hasNext() {
            // If current scanner has next then return true.
            if (null != nextElm) {
                return true;
            }
            while (scannerIterator.hasNext()) {
                final Entry<Key, Value> entry = scannerIterator.next();
                try {
                    nextElm = elementConverter.getFullElement(
                            entry.getKey(),
                            entry.getValue(),
                            includeMatchedVertex);
                } catch (final AccumuloElementConversionException e) {
                    LOGGER.error("Failed to re-create an element from a key value entry set returning next element as null",
                            e);
                    continue;
                }
                doTransformation(nextElm);
                if (doPostFilter(nextElm)) {
                    ViewUtil.removeProperties(operation.getView(), nextElm);
                    return true;
                } else {
                    nextElm = null;
                }
            }
            // If current scanner is spent then go back to the iterator
            // through the provided entities, and see if there are more.
            // If so create the next scanner, if there are no more entities
            // then return false.
            while (idsIterator.hasNext() && !scannerIterator.hasNext()) {
                count = 0;
                final Set<Range> ranges = new HashSet<>();
                while (idsIterator.hasNext() && count < store.getProperties().getMaxEntriesForBatchScanner()) {
                    count++;
                    try {
                        addToRanges(idsIterator.next(), ranges);
                    } catch (final RangeFactoryException e) {
                        LOGGER.error("Failed to create a range from given seed", e);
                    }
                }
                scanner.close();
                try {
                    scanner = getScanner(ranges);
                } catch (final TableNotFoundException | StoreException e) {
                    LOGGER.error("{} returning iterator doesn't have any more elements", e.getMessage(), e);
                    return false;
                }
                scannerIterator = scanner.iterator();
            }
            if (!scannerIterator.hasNext()) {
                scanner.close();
                return false;
            } else {
                return hasNext();
            }
        }

        @Override
        public Element next() {
            if (null == nextElm) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
            }
            Element nextReturn = nextElm;
            nextElm = null;
            return nextReturn;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Unable to remove elements from this iterator");
        }

        @Override
        public void close() {
            if (null != scanner) {
                scanner.close();
            }
        }
    }
}
