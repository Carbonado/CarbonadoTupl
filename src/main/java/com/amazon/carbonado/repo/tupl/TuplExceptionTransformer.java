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

import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockInterruptedException;
import org.cojen.tupl.LockTimeoutException;

import com.amazon.carbonado.FetchDeadlockException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchInterruptedException;
import com.amazon.carbonado.FetchTimeoutException;
import com.amazon.carbonado.PersistDeadlockException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistInterruptedException;
import com.amazon.carbonado.PersistTimeoutException;
import com.amazon.carbonado.RepositoryException;

import com.amazon.carbonado.spi.ExceptionTransformer;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class TuplExceptionTransformer extends ExceptionTransformer {
    static final TuplExceptionTransformer THE = new TuplExceptionTransformer();

    @Override
    protected FetchException transformIntoFetchException(Throwable e) {
        FetchException fe = super.transformIntoFetchException(e);
        if (fe != null) {
            return fe;
        }
        if (e instanceof LockFailureException) {
            if (e instanceof LockTimeoutException) {
                if (e instanceof DeadlockException) {
                    return new FetchDeadlockException(e);
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
                    return new PersistDeadlockException(e);
                }
                return new PersistTimeoutException(e);
            }
            if (e instanceof LockInterruptedException) {
                return new PersistInterruptedException(e);
            }
        }
        return null;
    }
}
