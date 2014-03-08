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

import java.io.File;
import java.io.IOException;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockUpgradeRule;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;

import com.amazon.carbonado.repo.indexed.IndexedRepositoryBuilder;

import com.amazon.carbonado.spi.AbstractRepositoryBuilder;

/**
 * Builder and configuration options for TuplRepository. Construct a non-durable instance as
 * follows:
 *
 * <pre>
 * Repository repo = TuplRepositoryBuilder.newRepository();
 * </pre>
 *
 * An example for constructing a durable instance:
 *
 * <pre>
 * TuplRepositoryBuilder bob = new TuplRepositoryBuilder();
 * bob.setName("test");
 * bob.setBaseFilePath("/var/lib/tupl/test");
 * bob.setMinCacheSize(100_000_000);
 * bob.setDurabilityNoFlush(true); // improves write performance
 * Repository repo = bob.build();
 * </pre>
 *
 * <p>
 * The following extra capabilities are supported:
 * <ul>
 * <li>{@link com.amazon.carbonado.capability.IndexInfoCapability IndexInfoCapability}
 * <li>{@link com.amazon.carbonado.capability.StorableInfoCapability StorableInfoCapability}
 * <li>{@link com.amazon.carbonado.capability.ShutdownCapability ShutdownCapability}
 * <li>{@link com.amazon.carbonado.layout.LayoutCapability LayoutCapability}
 * <li>{@link com.amazon.carbonado.sequence.SequenceCapability SequenceCapability}
 * <li>{@link DatabaseAccessCapability DatabaseAccessCapability}
 * </ul>
 *
 * @author Brian S O'Neill
 */
public final class TuplRepositoryBuilder extends AbstractRepositoryBuilder {
    private static int cNameId;

    /**
     * Convenience method to build a new non-durable TuplRepository. Maximum
     * allowed size is 100MB.
     */
    public static Repository newRepository() {
        return newRepository(100L * 1024 * 1024);
    }

    /**
     * Convenience method to build a new non-durable TuplRepository.
     *
     * @param maxSizeBytes maximum allowed size, in bytes
     */
    public static Repository newRepository(long maxSizeBytes) {
        try {
            TuplRepositoryBuilder builder = new TuplRepositoryBuilder();
            builder.setMaxCacheSize(maxSizeBytes);
            int id;
            synchronized (TuplRepositoryBuilder.class) {
                id = cNameId++;
            }
            builder.setName("tupl-" + id);
            return builder.build();
        } catch (RepositoryException e) {
            // Not expected.
            throw new RuntimeException(e);
        }
    }

    private String mName;
    private boolean mMaster;
    private DatabaseConfig mConfig;
    private Database mDatabase;
    private DurabilityMode mDurabilityMode;
    private LockUpgradeRule mLockUpgradeRule;

    private File mBaseFile;
    private File mDataFile;

    private boolean mIndexSupport = true;
    private boolean mIndexRepairEnabled = true;
    private double mIndexThrottle = 1.0;

    public TuplRepositoryBuilder() {
        mConfig = new DatabaseConfig();
        mMaster = true;
    }

    @Override
    public Repository build(AtomicReference<Repository> rootReference)
        throws ConfigurationException, RepositoryException
    {
        if (mIndexSupport) {
            // Wrap TuplRepository with IndexedRepository.

            // Temporarily set to false to avoid infinite recursion.
            mIndexSupport = false;
            try {
                IndexedRepositoryBuilder ixBuilder = new IndexedRepositoryBuilder();
                ixBuilder.setWrappedRepository(this);
                ixBuilder.setMaster(isMaster());
                ixBuilder.setIndexRepairEnabled(mIndexRepairEnabled);
                ixBuilder.setIndexRepairThrottle(mIndexThrottle);
                ixBuilder.setStrictTriggers
                    (mLockUpgradeRule == null || mLockUpgradeRule == LockUpgradeRule.STRICT);
                return ixBuilder.build(rootReference);
            } finally {
                mIndexSupport = true;
            }
        }

        assertReady();

        Log log;
        if (mBaseFile == null) {
            log = null;
        } else {
            log = LogFactory.getLog(TuplRepository.class);
        }

        Database db = mDatabase;
        if (db == null) {
            if (log != null) {
                mConfig.eventListener(new LogEventListener(log, mName));
            }
            try {
                db = Database.open(mConfig);
            } catch (IOException e) {
                throw new TuplExceptionTransformer(null).toRepositoryException(e);
            }
        }

        Repository repo = new TuplRepository
            (mName, mMaster, getTriggerFactories(), rootReference, db, log);

        rootReference.set(repo);
        return repo;
    }

    @Override
    public String getName() {
        return mName;
    }

    @Override
    public void setName(String name) {
        mName = name;
    }

    @Override
    public boolean isMaster() {
        return mMaster;
    }

    @Override
    public void setMaster(boolean b) {
        mMaster = b;
    }

    public File getBaseFile() {
        return mBaseFile;
    }

    /**
     * Set the base file name for the database, which must reside in an
     * ordinary file directory. If no base file is provided, database is
     * non-durable and cannot exceed the size of the cache.
     */
    public void setBaseFile(File file) {
        mBaseFile = file;
        mConfig.baseFile(file);
    }

    /**
     * Set the base file name for the database, which must reside in an
     * ordinary file directory. If no base file is provided, database is
     * non-durable and cannot exceed the size of the cache.
     */
    public void setBaseFilePath(String path) {
        setBaseFile(new File(path));
    }

    public File getDataFile() {
        return mDataFile == null ? mBaseFile : mDataFile;
    }

    /**
     * Set the data file for the database, which by default resides in the same
     * directory as the base file. The data file can be in a separate
     * directory, and it can even be a raw block device.
     */
    public void setDataFile(File file) {
        mDataFile = file;
        mConfig.dataFile(file);
    }

    /**
     * Set the data file for the database, which by default resides in the same
     * directory as the base file. The data file can be in a separate
     * directory, and it can even be a raw block device.
     */
    public void setDataFilePath(String path) {
        setDataFile(new File(path));
    }

    /**
     * Set the minimum cache size, overriding the default.
     *
     * @param minBytes cache size, in bytes
     */
    public void setMinCacheSize(long minBytes) {
        mConfig.minCacheSize(minBytes);
    }

    /**
     * Set the maximum cache size, overriding the default.
     *
     * @param maxBytes cache size, in bytes
     */
    public void setMaxCacheSize(long maxBytes) {
        mConfig.maxCacheSize(maxBytes);
    }

    /**
     * Set the default durability mode to sync, which ensures all modifications
     * are persisted to non-volatile storage.
     *
     * <p>If database itself is non-durabile, durability modes are ignored.
     *
     * @param b pass true for sync, false for no-sync
     */
    public void setDurabilitySync(boolean b) {
        durabilityMode(DurabilityMode.SYNC, b, DurabilityMode.NO_SYNC);
    }

    /**
     * Set the default durability mode to no-sync, which permits the operating
     * system to lazily persist modifications to non-volatile storage. This
     * mode is vulnerable to power failures and operating system crashes. These
     * events can cause recently committed transactions to get lost.
     *
     * <p>If database itself is non-durabile, durability modes are ignored.
     *
     * @param b pass true for no-sync, false for sync
     */
    public void setDurabilityNoSync(boolean b) {
        durabilityMode(DurabilityMode.NO_SYNC, b, DurabilityMode.SYNC);
    }

    /**
     * Set the default durability mode to no-flush, writes modifications to the
     * file system when the in-process buffer is full. This mode is vulnerable
     * to power failures, operating system crashes, and process crashes. These
     * events can cause recently committed transactions to get lost. When the
     * process exits cleanly, a shutdown hook switches this mode to behave like
     * no-sync and flushes the log.
     *
     * <p>If database itself is non-durabile, durability modes are ignored.
     *
     * @param b must be true
     */
    public void setDurabilityNoFlush(boolean b) {
        durabilityMode(DurabilityMode.NO_FLUSH, b, null);
    }

    /**
     * Set the default durability mode to no-redo, which doesn't write anything
     * to the redo log. An unlogged transaction does not become durable until a
     * checkpoint is performed. In addition to the vulnerabilities of no-flush
     * mode, no-redo mode can lose recently committed transactions when the
     * process exits.
     *
     * <p>If database itself is non-durabile, durability modes are ignored.
     *
     * @param b must be true
     */
    public void setDurabilityNoRedo(boolean b) {
        durabilityMode(DurabilityMode.NO_REDO, b, null);
    }

    private void durabilityMode(DurabilityMode mode, boolean b, DurabilityMode alt) {
        if (mDurabilityMode != null) {
            throw new IllegalArgumentException("Durability mode is already set");
        }
        if (!b) {
            if (alt == null) {
                throw new IllegalArgumentException("Setting must be true");
            }
            mode = alt;
        }
        mConfig.durabilityMode(mode);
        mDurabilityMode = mode;
    }

    /**
     * Set the lock upgrade rule to strict, which rejects shared lock upgrade attempts as
     * illegal.
     *
     * @param b pass true for strict, false for lenient
     */
    public void setLockUpgradeStrict(boolean b) {
        lockUpgradeRule(LockUpgradeRule.STRICT, b, LockUpgradeRule.LENIENT);
    }

    /**
     * Set the lock upgrade rule to lenient, which accepts shared lock upgrade attempts when
     * there are no conflicts. Any conflict causes an illegal lock upgrade exception to be
     * thrown.
     *
     * @param b pass true for lenient, false for strict
     */
    public void setLockUpgradeLenient(boolean b) {
        lockUpgradeRule(LockUpgradeRule.LENIENT, b, LockUpgradeRule.STRICT);
    }

    /**
     * Set the lock upgrade rule to unchecked, which always accepts shared lock upgrade
     * attempts, and is deadlock prone.
     *
     * @param b must be true
     */
    public void setLockUpgradeUnchecked(boolean b) {
        lockUpgradeRule(LockUpgradeRule.UNCHECKED, b, null);
    }

    private void lockUpgradeRule(LockUpgradeRule rule, boolean b, LockUpgradeRule alt) {
        if (mLockUpgradeRule != null) {
            throw new IllegalArgumentException("Lock upgrade rule is already set");
        }
        if (!b) {
            if (alt == null) {
                throw new IllegalArgumentException("Setting must be true");
            }
            rule = alt;
        }
        mConfig.lockUpgradeRule(rule);
        mLockUpgradeRule = rule;
    }

    /**
     * Set the default lock acquisition timeout, which is 1000 milliseconds if
     * not overridden. A negative timeout is infinite.
     */
    public void setLockTimeoutMillis(long millis) {
        mConfig.lockTimeout(millis, TimeUnit.MILLISECONDS);
    }

    /**
     * Set the rate at which checkpoints are automatically performed. Default
     * rate is 1000 milliseconds. Pass a negative value to disable automatic
     * checkpoints.
     */
    public void setCheckpointRateMillis(long millis) {
        mConfig.checkpointRate(millis, TimeUnit.MILLISECONDS);
    }

    /**
     * Set the page size, which is 4096 bytes by default.
     */
    public void setPageSize(int size) {
        mConfig.pageSize(size);
    }

    /**
     * Use the given config object instead of the methods on this
     * builder. Instance is not cloned.
     */
    public void setConfig(DatabaseConfig config) {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        mConfig = config;
    }

    public void setDatabase(Database db) {
        if (db == null) {
            throw new IllegalArgumentException();
        }
        mDatabase = db;
    }

    /**
     * By default, index repair is enabled. In this mode, the first time a
     * Storable type is used, new indexes are populated and old indexes are
     * removed. Until finished, access to the Storable is blocked.
     *
     * <p>When index repair is disabled, the Storable is immediately
     * available. This does have consequences, however. The set of indexes
     * available for queries is defined by the <i>intersection</i> of the old
     * and new index sets. The set of indexes that are kept up-to-date is
     * defined by the <i>union</i> of the old and new index sets.
     *
     * <p>While index repair is disabled, another process can safely repair the
     * indexes in the background. When it is complete, index repair can be
     * enabled for this repository too.
     */
    public void setIndexRepairEnabled(boolean enabled) {
        mIndexRepairEnabled = enabled;
    }

    /**
     * Sets the throttle parameter used when indexes are added, dropped or bulk
     * repaired. By default this value is 1.0, or maximum speed.
     *
     * @param desiredSpeed 1.0 = perform work at full speed,
     * 0.5 = perform work at half speed, 0.0 = fully suspend work
     */
    public void setIndexRepairThrottle(double desiredSpeed) {
        mIndexThrottle = desiredSpeed;
    }
}
