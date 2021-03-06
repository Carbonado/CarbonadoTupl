/*
 * Copyright 2013 Amazon Technologies, Inc. or its affiliates.
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

import java.util.Formatter;

import java.util.logging.Level;

import org.apache.commons.logging.Log;

import org.cojen.tupl.Database;
import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LogEventListener implements EventListener {
    private final Log mLog;
    private final String mName;
    private final TuplPanicHandler mPanicHandler;

    private Database mDatabase;

    public LogEventListener(Log log, String name) {
        this(log, name, null);
    }

    /**
     * @param log optional
     * @param panicHandler optional
     */
    LogEventListener(Log log, String name, TuplPanicHandler panicHandler) {
        mLog = log;
        mName = name;
        mPanicHandler = panicHandler;
    }

    void setDatabase(Database db) {
        mDatabase = db;
    }

    @Override
    public void notify(EventType type, String message, Object... args) {
        int intLevel = type.level.intValue();

        Log log = mLog;
        if (log != null) {
            if (intLevel <= Level.INFO.intValue()) {
                if (type.category == EventType.Category.CHECKPOINT) {
                    if (log.isDebugEnabled()) {
                        log.debug(format(type, message, args));
                    }
                } else if (log.isInfoEnabled()) {
                    log.info(format(type, message, args));
                }
            } else if (intLevel <= Level.WARNING.intValue()) {
                if (log.isWarnEnabled()) {
                    log.warn(format(type, message, args));
                }
            } else if (intLevel <= Level.SEVERE.intValue()) {
                if (log.isFatalEnabled()) {
                    log.fatal(format(type, message, args));
                }
            }
        }

        if (intLevel > Level.WARNING.intValue() && mPanicHandler != null) {
            mPanicHandler.onPanic(mDatabase, type, message, args);
        }
    }

    private String format(EventType type, String message, Object... args) {
        StringBuilder b = new StringBuilder();
        b.append('"').append(mName).append("\" ").append(type.category.toString()).append(": ");
        return new Formatter(b).format(message, args).toString();
    }
}
