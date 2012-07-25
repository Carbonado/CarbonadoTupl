/*
 * Copyright 2012 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
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

package com.amazon.carbonado.repo.tupl;

import org.cojen.tupl.Cursor;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.raw.RawCursor;
import com.amazon.carbonado.raw.RawUtil;

import com.amazon.carbonado.txn.TransactionScope;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class TuplCursor<S extends Storable> extends RawCursor<S> {
    private final TransactionScope<TuplTransaction> mScope;
    private final TuplStorage<S> mStorage;

    private final boolean mInclusiveStart;
    private final boolean mInclusiveEnd;

    private final Cursor mSource;

    TuplCursor(TransactionScope<TuplTransaction> scope,
               byte[] startBound, boolean inclusiveStart,
               byte[] endBound, boolean inclusiveEnd,
               boolean reverse,
               TuplStorage<S> storage)
        throws Exception
    {
        super(null,
              startBound, (!reverse) | inclusiveStart,
              endBound, reverse | inclusiveEnd,
              0, reverse);

        mScope = scope;
        mStorage = storage;
        mInclusiveStart = inclusiveStart;
        mInclusiveEnd = inclusiveEnd;
        scope.register(storage.getStorableType(), this);

        TuplTransaction txn = scope.getTxn();
        mSource = storage.mIx.newCursor(txn == null ? null : txn.mTxn);
    }

    @Override
    public void close() throws FetchException {
        try {
            super.close();
        } finally {
            mScope.unregister(mStorage.getStorableType(), this);
        }
    }

    @Override
    protected void release() throws FetchException {
        mSource.reset();
        mSource.link(null);
    }

    @Override
    protected byte[] getCurrentKey() throws FetchException {
        return mSource.key();
    }

    @Override
    protected byte[] getCurrentValue() throws FetchException {
        return mSource.value();
    }

    @Override
    protected void disableKeyAndValue() {
        mSource.autoload(false);
    }

    @Override
    protected void disableValue() {
        mSource.autoload(false);
    }

    @Override
    protected void enableKeyAndValue() throws FetchException {
        mSource.autoload(true);
    }

    @Override
    protected S instantiateCurrent() throws FetchException {
        return mStorage.instantiate(mSource.key(), mSource.value());
    }

    @Override
    protected boolean toFirst() throws FetchException {
        try {
            mSource.first();
            return mSource.value() != null;
        } catch (Exception e) {
            throw TuplExceptionTransformer.THE.toFetchException(e);
        }
    }

    @Override
    protected boolean toFirst(byte[] key) throws FetchException {
        try {
            if (mInclusiveStart) {
                mSource.findGe(key);
            } else {
                mSource.findGt(key);
            }
            return mSource.value() != null;
        } catch (Exception e) {
            throw TuplExceptionTransformer.THE.toFetchException(e);
        }
    }

    @Override
    protected boolean toLast() throws FetchException {
        try {
            mSource.last();
            return mSource.value() != null;
        } catch (Exception e) {
            throw TuplExceptionTransformer.THE.toFetchException(e);
        }
    }

    @Override
    protected boolean toLast(byte[] key) throws FetchException {
        try {
            if (mInclusiveEnd) {
                // Moving to the last entry of a range is a special case. Add
                // one to the key and find the highest which is less than it.
                // This destroys the caller's key value, but the toLast(byte[])
                // contract allows this.
                if (RawUtil.increment(key)) {
                    mSource.findLe(key);
                } else {
                    // This point is reached upon overflow, because key looked
                    // like: 0xff, 0xff, 0xff, 0xff...  So moving to the
                    // absolute last is just fine.
                    mSource.last();
                }
            } else {
                mSource.findLt(key);
            }
            return mSource.value() != null;
        } catch (Exception e) {
            throw TuplExceptionTransformer.THE.toFetchException(e);
        }
    }

    @Override
    protected boolean toNext() throws FetchException {
        try {
            mSource.next();
            return mSource.value() != null;
        } catch (Exception e) {
            throw TuplExceptionTransformer.THE.toFetchException(e);
        }
    }

    @Override
    protected boolean toPrevious() throws FetchException {
        try {
            mSource.previous();
            return mSource.value() != null;
        } catch (Exception e) {
            throw TuplExceptionTransformer.THE.toFetchException(e);
        }
    }
}
