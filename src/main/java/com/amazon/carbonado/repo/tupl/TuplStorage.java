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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.cojen.classfile.TypeDesc;

import org.cojen.tupl.Index;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.capability.IndexInfo;

import com.amazon.carbonado.cursor.ControllerCursor;
import com.amazon.carbonado.cursor.EmptyCursor;
import com.amazon.carbonado.cursor.MergeSortBuffer;
import com.amazon.carbonado.cursor.SingletonCursor;
import com.amazon.carbonado.cursor.SortBuffer;

import com.amazon.carbonado.filter.Filter;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.layout.Layout;
import com.amazon.carbonado.layout.LayoutFactory;
import com.amazon.carbonado.layout.Unevolvable;

import com.amazon.carbonado.lob.Blob;
import com.amazon.carbonado.lob.Clob;

import com.amazon.carbonado.qe.BoundaryType;
import com.amazon.carbonado.qe.QueryEngine;
import com.amazon.carbonado.qe.QueryExecutorFactory;
import com.amazon.carbonado.qe.StorableIndexSet;
import com.amazon.carbonado.qe.StorageAccess;

import com.amazon.carbonado.raw.StorableCodec;
import com.amazon.carbonado.raw.StorableCodecFactory;
import com.amazon.carbonado.raw.RawSupport;
import com.amazon.carbonado.raw.RawUtil;

import com.amazon.carbonado.sequence.SequenceValueProducer;

import com.amazon.carbonado.spi.IndexInfoImpl;
import com.amazon.carbonado.spi.TriggerManager;

import com.amazon.carbonado.txn.TransactionScope;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class TuplStorage<S extends Storable> implements Storage<S>, StorageAccess<S> {
    final Index mIx;
    final TuplRepository mRepo;
    final Class<S> mType;
    final RawSupport<S> mSupport;
    final QueryEngine<S> mQueryEngine;
    final StorableCodec<S> mStorableCodec;
    final StorableIndex<S> mPrimaryKeyIndex;
    final TriggerManager<S> mTriggerManager;

    TuplStorage(TuplRepository repo, Class<S> type) throws Exception {
        mRepo = repo;
        mType = type;
        mSupport = new Support(repo);
        mQueryEngine = new QueryEngine<S>(type, repo);
        mTriggerManager = new TriggerManager<S>();

        StorableInfo<S> info = StorableIntrospector.examine(type);
        StorableIndex<S> desiredPkIndex = desiredPkIndex(info);

        StorableCodecFactory codecFactory = repo.getStorableCodecFactory();
        final Layout layout = getLayout(codecFactory);

        String ixName = codecFactory.getStorageName(type);
        if (ixName == null) {
            ixName = type.getName();
        }

        mIx = repo.mDb.openIndex(ixName);

        boolean isPrimaryEmpty;
        {
            org.cojen.tupl.Cursor c = mIx.newCursor(null);
            try {
                c.first();
                isPrimaryEmpty = c.key() == null;
            } finally {
                c.reset();
            }
        }

        StoredPrimaryIndexInfo primaryInfo = registerPrimaryIndex(layout);
        StorableIndex<S> pkIndex;

        if (!isPrimaryEmpty && primaryInfo != null
            && primaryInfo.getIndexNameDescriptor() != null)
        {
            // Entries already exist, so primary key format is locked in.
            pkIndex = verifyPrimaryKey(info, desiredPkIndex,
                                       primaryInfo.getIndexNameDescriptor(),
                                       primaryInfo.getIndexTypeDescriptor());
        } else {
            pkIndex = desiredPkIndex;

            if (primaryInfo != null &&
                (!pkIndex.getNameDescriptor().equals(primaryInfo.getIndexNameDescriptor()) ||
                 !pkIndex.getTypeDescriptor().equals(primaryInfo.getIndexTypeDescriptor())))
            {
                primaryInfo.setIndexNameDescriptor(pkIndex.getNameDescriptor());
                primaryInfo.setIndexTypeDescriptor(pkIndex.getTypeDescriptor());

                Repository rootRepo = repo.getRootRepository();
                Transaction txn = rootRepo.enterTopTransaction(IsolationLevel.REPEATABLE_READ);
                try {
                    txn.setForUpdate(true);
                    primaryInfo.update();
                    txn.commit();
                } finally {
                    txn.exit();
                }
            }
        }

        mStorableCodec = codecFactory.createCodec(type, pkIndex, repo.mIsMaster, layout, mSupport);
        mPrimaryKeyIndex = mStorableCodec.getPrimaryKeyIndex();

        // Don't install automatic triggers until we're completely ready.
        mTriggerManager.addTriggers(type, repo.mTriggerFactories);
    }

    private StorableIndex<S> desiredPkIndex(StorableInfo<S> info) {
        // In order to select the best index for the primary key, allow all
        // indexes to be considered.
        StorableIndexSet<S> indexSet = new StorableIndexSet<S>();
        indexSet.addIndexes(info);
        indexSet.addAlternateKeys(info);
        indexSet.addPrimaryKey(info);

        indexSet.reduce(Direction.ASCENDING);

        return indexSet.findPrimaryKeyIndex(info);
    }

    @Override
    public Class<S> getStorableType() {
        return mType;
    }

    @Override
    public S prepare() {
        return mStorableCodec.instantiate();
    }

    S instantiate(byte[] key, byte[] value) throws FetchException {
        return mStorableCodec.instantiate(key, value);
    }

    @Override
    public Query<S> query() throws FetchException {
        return mQueryEngine.query();
    }

    @Override
    public Query<S> query(String filter) throws FetchException {
        return mQueryEngine.query(filter);
    }

    @Override
    public Query<S> query(Filter<S> filter) throws FetchException {
        return mQueryEngine.query(filter);
    }

    @Override
    public void truncate() throws PersistException {
        if (mTriggerManager.getDeleteTrigger() != null) {
            try {
                Cursor<S> cursor = query().fetch();
                try {
                    while (cursor.hasNext()) {
                        cursor.next().tryDelete();
                    }
                } finally {
                    cursor.close();
                }
                return;
            } catch (FetchException e) {
                throw e.toPersistException();
            }
        }

        org.cojen.tupl.Cursor cursor = mIx.newCursor(org.cojen.tupl.Transaction.BOGUS);
        try {
            cursor.autoload(false);
            cursor.first();
            while (cursor.key() != null) {
                cursor.store(null);
                cursor.next();
            }
        } catch (Exception e) {
            throw TuplExceptionTransformer.THE.toPersistException(e);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public boolean addTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.addTrigger(trigger);
    }

    @Override
    public boolean removeTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.removeTrigger(trigger);
    }

    @Override
    public QueryExecutorFactory<S> getQueryExecutorFactory() {
        return mQueryEngine;
    }

    @Override
    public Collection<StorableIndex<S>> getAllIndexes() {
        return Collections.singletonList(mPrimaryKeyIndex);
    }

    @Override
    public Storage<S> storageDelegate(StorableIndex<S> index) {
        // We're the grunt and don't delegate.
        return null;
    }

    @Override
    public SortBuffer<S> createSortBuffer() {
        return new MergeSortBuffer<S>();
    }

    @Override
    public SortBuffer<S> createSortBuffer(Query.Controller controller) {
        return new MergeSortBuffer<S>(controller);
    }

    @Override
    public long countAll() throws FetchException {
        // Return -1 to indicate default algorithm should be used.
        return -1;
    }

    @Override
    public long countAll(Query.Controller controller) throws FetchException {
        // Return -1 to indicate default algorithm should be used.
        return -1;
    }

    @Override
    public Cursor<S> fetchAll() throws FetchException {
        return fetchAll(null);
    }

    @Override
    public Cursor<S> fetchAll(Query.Controller controller) throws FetchException {
        return fetchSubset(null, null,
                           BoundaryType.OPEN, null,
                           BoundaryType.OPEN, null,
                           false, false,
                           controller);
    }

    @Override
    public Cursor<S> fetchOne(StorableIndex<S> index,
                              Object[] identityValues)
        throws FetchException
    {
        return fetchOne(index, identityValues, null);
    }

    @Override
    public Cursor<S> fetchOne(StorableIndex<S> index,
                              Object[] identityValues,
                              Query.Controller controller)
        throws FetchException
    {
        // Note: Controller is never called.
        byte[] key = mStorableCodec.encodePrimaryKey(identityValues);
        byte[] value = mSupport.tryLoad(null, key);
        if (value == null) {
            return EmptyCursor.the();
        }
        return new SingletonCursor<S>(mStorableCodec.instantiate(key, value));
    }

    @Override
    public Query<?> indexEntryQuery(StorableIndex<S> index) {
        return null;
    }

    @Override
    public Cursor<S> fetchFromIndexEntryQuery(StorableIndex<S> index, Query<?> indexEntryQuery) {
        // This method should never be called since null was returned by indexEntryQuery.
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor<S> fetchFromIndexEntryQuery(StorableIndex<S> index, Query<?> indexEntryQuery,
                                              Query.Controller controller)
    {
        // This method should never be called since null was returned by indexEntryQuery.
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor<S> fetchSubset(StorableIndex<S> index,
                                 Object[] identityValues,
                                 BoundaryType rangeStartBoundary,
                                 Object rangeStartValue,
                                 BoundaryType rangeEndBoundary,
                                 Object rangeEndValue,
                                 boolean reverseRange,
                                 boolean reverseOrder)
        throws FetchException
    {
        if (reverseRange) {
            {
                BoundaryType temp = rangeStartBoundary;
                rangeStartBoundary = rangeEndBoundary;
                rangeEndBoundary = temp;
            }

            {
                Object temp = rangeStartValue;
                rangeStartValue = rangeEndValue;
                rangeEndValue = temp;
            }
        }

        StorableCodec<S> codec = mStorableCodec;

        final byte[] identityKey;
        if (identityValues == null || identityValues.length == 0) {
            identityKey = codec.encodePrimaryKeyPrefix();
        } else {
            identityKey = codec.encodePrimaryKey(identityValues, 0, identityValues.length);
        }

        final byte[] startBound;
        if (rangeStartBoundary == BoundaryType.OPEN) {
            startBound = identityKey;
        } else {
            startBound = createBound(identityValues, identityKey, rangeStartValue, codec);
            if (!reverseOrder && rangeStartBoundary == BoundaryType.EXCLUSIVE) {
                // If key is composite and partial, need to skip trailing
                // unspecified keys by adding one and making inclusive.
                if (!RawUtil.increment(startBound)) {
                    return EmptyCursor.the();
                }
                rangeStartBoundary = BoundaryType.INCLUSIVE;
            }
        }

        final byte[] endBound;
        if (rangeEndBoundary == BoundaryType.OPEN) {
            endBound = identityKey;
        } else {
            endBound = createBound(identityValues, identityKey, rangeEndValue, codec);
            if (reverseOrder && rangeEndBoundary == BoundaryType.EXCLUSIVE) {
                // If key is composite and partial, need to skip trailing
                // unspecified keys by subtracting one and making inclusive.
                if (!RawUtil.decrement(endBound)) {
                    return EmptyCursor.the();
                }
                rangeEndBoundary = BoundaryType.INCLUSIVE;
            }
        }

        try {
            return new TuplCursor<S>
                (mRepo.localTransactionScope(),
                 startBound, rangeStartBoundary != BoundaryType.EXCLUSIVE,
                 endBound, rangeEndBoundary != BoundaryType.EXCLUSIVE,
                 reverseOrder,
                 this);
        } catch (Exception e) {
            throw TuplExceptionTransformer.THE.toFetchException(e);
        }
    }

    @Override
    public Cursor<S> fetchSubset(StorableIndex<S> index,
                                 Object[] identityValues,
                                 BoundaryType rangeStartBoundary,
                                 Object rangeStartValue,
                                 BoundaryType rangeEndBoundary,
                                 Object rangeEndValue,
                                 boolean reverseRange,
                                 boolean reverseOrder,
                                 Query.Controller controller)
        throws FetchException
    {
        return ControllerCursor.apply(fetchSubset(index,
                                                  identityValues,
                                                  rangeStartBoundary,
                                                  rangeStartValue,
                                                  rangeEndBoundary,
                                                  rangeEndValue,
                                                  reverseRange,
                                                  reverseOrder),
                                      controller);
    }

    private byte[] createBound(Object[] exactValues, byte[] exactKey, Object rangeValue,
                               StorableCodec<S> codec)
    {
        Object[] values = {rangeValue};
        if (exactValues == null || exactValues.length == 0) {
            return codec.encodePrimaryKey(values, 0, 1);
        }

        byte[] rangeKey = codec.encodePrimaryKey
            (values, exactValues.length, exactValues.length + 1);
        byte[] bound = new byte[exactKey.length + rangeKey.length];
        System.arraycopy(exactKey, 0, bound, 0, exactKey.length);
        System.arraycopy(rangeKey, 0, bound, exactKey.length, rangeKey.length);
        return bound;
    }

    IndexInfo[] getIndexInfo() {
        StorableIndex<S> pkIndex = mPrimaryKeyIndex;

        if (pkIndex == null) {
            return new IndexInfo[0];
        }

        int i = pkIndex.getPropertyCount();
        String[] propertyNames = new String[i];
        Direction[] directions = new Direction[i];
        while (--i >= 0) {
            propertyNames[i] = pkIndex.getProperty(i).getName();
            directions[i] = pkIndex.getPropertyDirection(i);
        }

        return new IndexInfo[] {
            new IndexInfoImpl(getStorableType().getName(), true, true, propertyNames, directions)
        };
    }

    Layout getLayout(StorableCodecFactory codecFactory) throws RepositoryException {
        if (Unevolvable.class.isAssignableFrom(getStorableType())) {
            // Don't record generation for storables marked as unevolvable.
            return null;
        }

        LayoutFactory factory;
        try {
            factory = mRepo.getLayoutFactory();
        } catch (SupportException e) {
            // Metadata repository does not support layout storables, so it
            // cannot support generations.
            return null;
        }

        Class<S> type = getStorableType();
        return factory.layoutFor(type, codecFactory.getLayoutOptions(type));
    }

    /**
     * Note: returned StoredPrimaryIndexInfo does not have name and type
     * descriptors saved yet.
     *
     * @return null if type cannot be registered
     */
    private StoredPrimaryIndexInfo registerPrimaryIndex(Layout layout) throws Exception {
        if (getStorableType() == StoredPrimaryIndexInfo.class) {
            // Can't register itself in itself.
            return null;
        }

        Repository repo = mRepo.getRootRepository();

        StoredPrimaryIndexInfo info;
        try {
            info = repo.storageFor(StoredPrimaryIndexInfo.class).prepare();
        } catch (SupportException e) {
            return null;
        }
        info.setIndexName(getStorableType().getName());

        Transaction txn = repo.enterTopTransaction(IsolationLevel.REPEATABLE_READ);
        try {
            txn.setForUpdate(true);
            if (!info.tryLoad()) {
                if (layout == null) {
                    info.setEvolutionStrategy(StoredPrimaryIndexInfo.EVOLUTION_NONE);
                } else {
                    info.setEvolutionStrategy(StoredPrimaryIndexInfo.EVOLUTION_STANDARD);
                }
                info.setCreationTimestamp(System.currentTimeMillis());
                info.setVersionNumber(0);
                info.insert();
            }
            txn.commit();
        } finally {
            txn.exit();
        }

        return info;
    }

    private void unregisterPrimaryIndex(String name) throws RepositoryException {
        if (getStorableType() == StoredPrimaryIndexInfo.class) {
            // Can't unregister when register wasn't allowed.
            return;
        }

        Repository repo = mRepo.getRootRepository();

        StoredPrimaryIndexInfo info;
        try {
            info = repo.storageFor(StoredPrimaryIndexInfo.class).prepare();
        } catch (SupportException e) {
            return;
        }
        info.setIndexName(name);

        Transaction txn = repo.enterTopTransaction(IsolationLevel.REPEATABLE_READ);
        try {
            info.delete();
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    private StorableIndex<S> verifyPrimaryKey(StorableInfo<S> info,
                                              StorableIndex<S> desiredPkIndex,
                                              String nameDescriptor,
                                              String typeDescriptor)
        throws SupportException
    {
        StorableIndex<S> pkIndex;
        try {
            pkIndex = StorableIndex.parseNameDescriptor(nameDescriptor, info);
        } catch (IllegalArgumentException e) {
            throw new SupportException
                ("Existing primary key apparently refers to properties which " +
                 "no longer exist. Primary key cannot change if Storage<" +
                 info.getStorableType().getName() + "> is not empty. " +
                 "Primary key name descriptor: " + nameDescriptor + ", error: " +
                 e.getMessage());
        }

        if (!nameDescriptor.equals(desiredPkIndex.getNameDescriptor())) {
            throw new SupportException
                (buildIndexMismatchMessage(info, pkIndex, desiredPkIndex, null, false));
        }

        if (!typeDescriptor.equals(desiredPkIndex.getTypeDescriptor())) {
            throw new SupportException
                (buildIndexMismatchMessage(info, pkIndex, desiredPkIndex, typeDescriptor, true));
        }
    
        return pkIndex;
    }

    private String buildIndexMismatchMessage(StorableInfo<S> info,
                                             StorableIndex<S> pkIndex,
                                             StorableIndex<S> desiredPkIndex,
                                             String typeDescriptor,
                                             boolean showDesiredType)
    {
        StringBuilder message = new StringBuilder();
        message.append("Cannot change primary key if Storage<" + info.getStorableType().getName() +
                       "> is not empty. Primary key was ");
        appendIndexDecl(message, pkIndex, typeDescriptor, false);
        message.append(", but new specification is ");
        appendIndexDecl(message, desiredPkIndex, null, showDesiredType);
        return message.toString();
    }

    private void appendIndexDecl(StringBuilder buf, StorableIndex<S> index,
                                 String typeDescriptor, boolean showDesiredType)
    {
        buf.append('[');
        int count = index.getPropertyCount();

        TypeDesc[] types = null;
        boolean[] nullable = null;

        if (typeDescriptor != null) {
            types = new TypeDesc[count];
            nullable = new boolean[count];

            try {
                for (int i=0; i<count; i++) {
                    if (typeDescriptor.charAt(0) == 'N') {
                        typeDescriptor = typeDescriptor.substring(1);
                        nullable[i] = true;
                    }

                    String typeStr;

                    if (typeDescriptor.charAt(0) == 'L') {
                        int end = typeDescriptor.indexOf(';');
                        typeStr = typeDescriptor.substring(0, end + 1);
                        typeDescriptor = typeDescriptor.substring(end + 1);
                    } else {
                        typeStr = typeDescriptor.substring(0, 1);
                        typeDescriptor = typeDescriptor.substring(1);
                    }

                    types[i] = TypeDesc.forDescriptor(typeStr);
                }
            } catch (IndexOutOfBoundsException e) {
            }
        }

        for (int i=0; i<count; i++) {
            if (i > 0) {
                buf.append(", ");
            }
            if (types != null) {
                if (nullable[i]) {
                    buf.append("@Nullable ");
                }
                buf.append(types[i].getFullName());
                buf.append(' ');
            } else if (showDesiredType) {
                if (index.getProperty(i).isNullable()) {
                    buf.append("@Nullable ");
                }
                buf.append(TypeDesc.forClass(index.getProperty(i).getType()).getFullName());
                buf.append(' ');
            }
            buf.append(index.getPropertyDirection(i).toCharacter());
            buf.append(index.getProperty(i).getName());
        }

        buf.append(']');
    }

    // Note: TuplStorage could just implement the RawSupport interface, but
    // then these hidden methods would be public. A simple cast of Storage to
    // RawSupport would expose them.
    private class Support implements RawSupport<S> {
        private final TuplRepository mRepo;
        private Map<String, ? extends StorableProperty<S>> mProperties;

        Support(TuplRepository repo) {
            mRepo = repo;
        }

        @Override
        public Repository getRootRepository() {
            return mRepo.getRootRepository();
        }

        @Override
        public boolean isPropertySupported(String name) {
            if (name == null) {
                return false;
            }
            if (mProperties == null) {
                mProperties = StorableIntrospector
                    .examine(getStorableType()).getAllProperties();
            }
            return mProperties.containsKey(name);
        }

        @Override
        public byte[] tryLoad(S storable, byte[] key) throws FetchException {
            try {
                TuplTransaction txn = mRepo.localTransactionScope().getTxn();
                return mIx.load(txn == null ? null : txn.mTxn, key);
            } catch (Throwable e) {
                throw TuplExceptionTransformer.THE.toFetchException(e);
            }
        }

        @Override
        public boolean tryInsert(S storable, byte[] key, byte[] value) throws PersistException {
            try {
                TuplTransaction txn = mRepo.localTransactionScope().getTxn();
                return mIx.insert(txn == null ? null : txn.mTxn, key, value);
            } catch (Throwable e) {
                throw TuplExceptionTransformer.THE.toPersistException(e);
            }
        }

        @Override
        public void store(S storable, byte[] key, byte[] value) throws PersistException {
            try {
                TuplTransaction txn = mRepo.localTransactionScope().getTxn();
                mIx.store(txn == null ? null : txn.mTxn, key, value);
            } catch (Throwable e) {
                throw TuplExceptionTransformer.THE.toPersistException(e);
            }
        }

        @Override
        public boolean tryDelete(S storable, byte[] key) throws PersistException {
            try {
                TuplTransaction txn = mRepo.localTransactionScope().getTxn();
                return mIx.delete(txn == null ? null : txn.mTxn, key);
            } catch (Throwable e) {
                throw TuplExceptionTransformer.THE.toPersistException(e);
            }
        }

        @Override
        public Blob getBlob(S storable, String name, long locator) throws FetchException {
            // FIXME
            throw null;
        }

        @Override
        public long getLocator(Blob blob) throws PersistException {
            // FIXME
            throw null;
        }

        @Override
        public Clob getClob(S storable, String name, long locator) throws FetchException {
            // FIXME
            throw null;
        }

        @Override
        public long getLocator(Clob clob) throws PersistException {
            // FIXME
            throw null;
        }

        @Override
        public void decode(S dest, int generation, byte[] data) throws CorruptEncodingException {
            mStorableCodec.decode(dest, generation, data);
        }

        @Override
        public SequenceValueProducer getSequenceValueProducer(String name)
            throws PersistException
        {
            try {
                return mRepo.getSequenceValueProducer(name);
            } catch (RepositoryException e) {
                throw e.toPersistException();
            }
        }

        @Override
        public Trigger<? super S> getInsertTrigger() {
            return mTriggerManager.getInsertTrigger();
        }

        @Override
        public Trigger<? super S> getUpdateTrigger() {
            return mTriggerManager.getUpdateTrigger();
        }

        @Override
        public Trigger<? super S> getDeleteTrigger() {
            return mTriggerManager.getDeleteTrigger();
        }

        @Override
        public Trigger<? super S> getLoadTrigger() {
            return mTriggerManager.getLoadTrigger();
        }

        @Override
        public void locallyDisableLoadTrigger() {
            mTriggerManager.locallyDisableLoad();
        }

        @Override
        public void locallyEnableLoadTrigger() {
            mTriggerManager.locallyEnableLoad();
        }
    }
}
