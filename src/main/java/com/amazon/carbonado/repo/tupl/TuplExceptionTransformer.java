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

import org.cojen.tupl.Database;
import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.DeadlockSet;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockInterruptedException;
import org.cojen.tupl.LockTimeoutException;
import org.cojen.tupl.UnmodifiableReplicaException;

import com.amazon.carbonado.FetchDeadlockException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchInterruptedException;
import com.amazon.carbonado.FetchTimeoutException;
import com.amazon.carbonado.PersistDeadlockException;
import com.amazon.carbonado.PersistDeniedException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistInterruptedException;
import com.amazon.carbonado.PersistTimeoutException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.spi.ExceptionTransformer;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class TuplExceptionTransformer extends ExceptionTransformer {
    private final TuplRepository mRepo;

    /**
     * @param repo non-null to allow deadlock exceptions to show more information
     */
    TuplExceptionTransformer(TuplRepository repo) {
        mRepo = repo;
    }

    @Override
    protected FetchException transformIntoFetchException(Throwable e) {
        FetchException fe = super.transformIntoFetchException(e);
        if (fe != null) {
            return fe;
        }
        if (e instanceof LockFailureException) {
            if (e instanceof LockTimeoutException) {
                if (e instanceof DeadlockException) {
                    String message = messageFrom((DeadlockException) e);
                    return message == null ? new FetchDeadlockException(e)
                        : new FetchDeadlockException(message);
                }
                return new FetchTimeoutException(e);
            }
            if (e instanceof LockInterruptedException) {
                return new FetchInterruptedException(e);
            }
        }
        return null;
    }

    @Override
    protected PersistException transformIntoPersistException(Throwable e) {
        PersistException pe = super.transformIntoPersistException(e);
        if (pe != null) {
            return pe;
        }
        if (e instanceof LockFailureException) {
            if (e instanceof LockTimeoutException) {
                if (e instanceof DeadlockException) {
                    String message = messageFrom((DeadlockException) e);
                    return message == null ? new PersistDeadlockException(e)
                        : new PersistDeadlockException(message);
                }
                return new PersistTimeoutException(e);
            }
            if (e instanceof LockInterruptedException) {
                return new PersistInterruptedException(e);
            }
        }
        if (e instanceof UnmodifiableReplicaException) {
            return new PersistDeniedException(e);
        }
        return null;
    }

    private String messageFrom(DeadlockException e) {
        if (mRepo == null) {
            return null;
        }

        DeadlockSet set = e.getDeadlockSet();
        int size;
        if (set == null || (size = set.size()) == 0) {
            return null;
        }

        try {
            Database db = mRepo.mDb;

            StringBuilder b = new StringBuilder(e.getShortMessage());

            b.append(" Deadlock set: ");
            b.append('[');

            for (int i=0; i<size; i++) {
                if (i > 0) {
                    b.append(", ");
                }
                b.append('{');
                Index ix = db.indexById(set.getIndexId(i));
                if (ix == null) {
                    return null;
                }
                TuplStorage storage = mRepo.storageByIndexId(ix.getId());
                Storable storable = storage.mStorableCodec.instantiate(set.getKey(i));
                b.append(storable);
                b.append('}');
            }

            b.append(']');

            return b.toString();
        } catch (Throwable e2) {
            return null;
        }
    }
}
