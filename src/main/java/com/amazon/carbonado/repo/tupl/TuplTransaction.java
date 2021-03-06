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

import org.cojen.tupl.LockMode;
import org.cojen.tupl.Transaction;

import com.amazon.carbonado.PersistException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class TuplTransaction {
    final Transaction mTxn;
    final LockMode mOriginalMode;

    private boolean mDone;

    TuplTransaction(Transaction txn, LockMode mode) {
        mTxn = txn;
        mOriginalMode = mode;
        txn.lockMode(mode);
    }

    void setForUpdate(boolean forUpdate) {
        mTxn.lockMode(forUpdate ? LockMode.UPGRADABLE_READ : mOriginalMode);
    }

    void commit() throws PersistException {
        if (!mDone) {
            try {
                mTxn.commit();
                mTxn.exit();
            } catch (Exception e) {
                throw new TuplExceptionTransformer(null).toPersistException(e);
            }
            mDone = true;
        }
    }

    void abort() throws PersistException {
        if (!mDone) {
            try {
                mTxn.exit();
            } catch (Exception e) {
                throw new TuplExceptionTransformer(null).toPersistException(e);
            }
            mDone = true;
        }
    }
}
