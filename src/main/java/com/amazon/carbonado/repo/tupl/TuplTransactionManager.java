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

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.Database;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Transaction;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;

import com.amazon.carbonado.txn.TransactionManager;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class TuplTransactionManager extends TransactionManager<TuplTransaction> {
    private final Database mDb;

    TuplTransactionManager(Database db) {
        mDb = db;
    }

    @Override
    protected IsolationLevel selectIsolationLevel(com.amazon.carbonado.Transaction parent,
                                                  IsolationLevel level)
    {
        if (level == null) {
            return parent == null ? IsolationLevel.REPEATABLE_READ : parent.getIsolationLevel();
        }
        return level.isAtMost(IsolationLevel.REPEATABLE_READ) ? level : null;
    }

    @Override
    protected boolean supportsForUpdate() {
        return true;
    }

    @Override
    protected TuplTransaction createTxn(TuplTransaction parent, IsolationLevel level)
        throws Exception
    {
        LockMode mode;
        switch (level) {
        case NONE:
            return null;
        case READ_UNCOMMITTED:
            mode = LockMode.READ_UNCOMMITTED;
            break;
        case READ_COMMITTED:
            mode = LockMode.READ_COMMITTED;
            break;
        case REPEATABLE_READ:
            mode = LockMode.REPEATABLE_READ;
            break;
        default:
            mode = LockMode.UPGRADABLE_READ;
            break;
        }

        Transaction txn;
        if (parent == null) {
            txn = mDb.newTransaction();
        } else {
            txn = parent.mTxn;
            txn.enter();
        }

        return new TuplTransaction(txn, mode);
    }

    @Override
    protected TuplTransaction createTxn(TuplTransaction parent, IsolationLevel level,
                                        int timeout, TimeUnit unit)
        throws Exception
    {
        TuplTransaction txn = createTxn(parent, level);
        if (txn != null) {
            txn.mTxn.lockTimeout(timeout, unit);
        }
        return txn;
    }

    @Override
    protected void setForUpdate(TuplTransaction txn, boolean forUpdate) {
        txn.setForUpdate(forUpdate);
    }

    @Override
    protected boolean commitTxn(TuplTransaction txn) throws PersistException {
        txn.commit();
        return true;
    }

    @Override
    protected void abortTxn(TuplTransaction txn) throws PersistException {
        txn.abort();
    }
}
