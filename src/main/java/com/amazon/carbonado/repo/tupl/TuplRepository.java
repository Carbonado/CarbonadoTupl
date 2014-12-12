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

import java.util.ArrayList;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;

import org.cojen.util.SoftValueCache;

import org.cojen.tupl.Database;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.TriggerFactory;

import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;
import com.amazon.carbonado.capability.StorableInfoCapability;

import com.amazon.carbonado.info.StorableIntrospector;

import com.amazon.carbonado.layout.Layout;
import com.amazon.carbonado.layout.LayoutCapability;
import com.amazon.carbonado.layout.LayoutFactory;

import com.amazon.carbonado.qe.RepositoryAccess;
import com.amazon.carbonado.qe.StorageAccess;

import com.amazon.carbonado.raw.CompressedStorableCodecFactory;
import com.amazon.carbonado.raw.StorableCodecFactory;

import com.amazon.carbonado.sequence.SequenceCapability;
import com.amazon.carbonado.sequence.SequenceValueGenerator;
import com.amazon.carbonado.sequence.SequenceValueProducer;

import com.amazon.carbonado.spi.AbstractRepository;

import com.amazon.carbonado.txn.TransactionManager;
import com.amazon.carbonado.txn.TransactionScope;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class TuplRepository extends AbstractRepository<TuplTransaction>
    implements DatabaseAccessCapability,
               RepositoryAccess,
               IndexInfoCapability,
               LayoutCapability,
               SequenceCapability,
               StorableInfoCapability
{
    final boolean mIsMaster;
    final Iterable<TriggerFactory> mTriggerFactories;
    private final AtomicReference<Repository> mRootRef;
    final Database mDb;
    final Log mLog;

    private final TuplTransactionManager mTxnMgr;

    private final StorableCodecFactory mStorableCodecFactory;

    final TuplExceptionTransformer mExTransformer;

    private LayoutFactory mLayoutFactory;

    private final SoftValueCache<Long, TuplStorage> mIndexToStorageMap;

    TuplRepository(String name, boolean master, Iterable<TriggerFactory> triggerFactories,
                   AtomicReference<Repository> rootRef,
                   Database db, Log log)
    {
        super(name);
        mIsMaster = master;
        mTriggerFactories = triggerFactories;
        mRootRef = rootRef;
        mDb = db;
        mLog = log;
        setAutoShutdownEnabled(false);

        mTxnMgr = new TuplTransactionManager(db);

        mStorableCodecFactory = new CompressedStorableCodecFactory(null);

        mExTransformer = new TuplExceptionTransformer(this);

        mIndexToStorageMap = new SoftValueCache<Long, TuplStorage>(17);
    }

    @Override
    public Database getDatabase() {
        return mDb;
    }

    @Override
    public Repository getRootRepository() {
        return mRootRef.get();
    }

    @Override
    public <S extends Storable> StorageAccess<S> storageAccessFor(Class<S> type)
        throws RepositoryException
    {
        return (TuplStorage<S>) storageFor(type);
    }

    @Override
    public <S extends Storable> IndexInfo[] getIndexInfo(Class<S> storableType)
        throws RepositoryException
    {
        return ((TuplStorage) storageFor(storableType)).getIndexInfo();
    }

    @Override
    public Layout layoutFor(Class<? extends Storable> type)
        throws FetchException, PersistException
    {
        try {
            return ((TuplStorage) storageFor(type)).getLayout(mStorableCodecFactory);
        } catch (PersistException e) {
            throw e;
        } catch (RepositoryException e) {
            throw e.toFetchException();
        }
    }

    @Override
    public Layout layoutFor(Class<? extends Storable> type, int generation)
        throws FetchException
    {
        return mLayoutFactory.layoutFor(type, generation);
    }

    @Override
    protected TransactionManager<TuplTransaction> transactionManager() {
        return mTxnMgr;
    }

    @Override
    protected TransactionScope<TuplTransaction> localTransactionScope() {
        return mTxnMgr.localScope();
    }

    @Override
    protected void shutdownHook() {
        try {
            mDb.close();
        } catch (Exception e) {
        }
        mRootRef.set(null);
        mLayoutFactory = null;
    }

    @Override
    protected Log getLog() {
        return mLog;
    }

    @Override
    protected <S extends Storable> Storage<S> createStorage(Class<S> type)
        throws RepositoryException
    {
        try {
            TuplStorage<S> storage = new TuplStorage<S>(this, type);
            mIndexToStorageMap.put(new Long(storage.mIx.getId()), storage);
            return storage;
        } catch (Exception e) {
            throw mExTransformer.toRepositoryException(e);
        }
    }

    @Override
    protected SequenceValueProducer createSequenceValueProducer(String name)
        throws RepositoryException
    {
        return new SequenceValueGenerator(this, name);
    }

    @Override
    public String[] getUserStorableTypeNames() throws RepositoryException {
        Repository metaRepo = getRootRepository();

        Cursor<StoredPrimaryIndexInfo> cursor =
            metaRepo.storageFor(StoredPrimaryIndexInfo.class)
            .query().orderBy("indexName").fetch();

        try {
            ArrayList<String> names = new ArrayList<String>();
            while (cursor.hasNext()) {
                StoredPrimaryIndexInfo info = cursor.next();
                // Ordinary user types support evolution.
                if (info.getEvolutionStrategy() != StoredPrimaryIndexInfo.EVOLUTION_NONE) {
                    names.add(info.getIndexName());
                }
            }

            return names.toArray(new String[names.size()]);
        } finally {
            cursor.close();
        }
    }

    @Override
    public boolean isSupported(Class<Storable> type) {
        if (type == null) {
            return false;
        }
        StorableIntrospector.examine(type);
        return true;
    }

    @Override
    public boolean isPropertySupported(Class<Storable> type, String name) {
        if (type == null || name == null) {
            return false;
        }
        return StorableIntrospector.examine(type).getAllProperties().get(name) != null;
    }

    StorableCodecFactory getStorableCodecFactory() {
        return mStorableCodecFactory;
    }

    LayoutFactory getLayoutFactory() throws RepositoryException {
        if (mLayoutFactory == null) {
            mLayoutFactory = new LayoutFactory(getRootRepository());
        }
        return mLayoutFactory;
    }

    TuplStorage<?> storageByIndexId(long id) {
        return mIndexToStorageMap.get(id);
    }
}
