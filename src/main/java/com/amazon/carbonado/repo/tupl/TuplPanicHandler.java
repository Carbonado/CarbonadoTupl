/*
 * Copyright 2015 Amazon Technologies, Inc. or its affiliates.
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
import org.cojen.tupl.EventType;

/**
 * Interface for a generic panic handler for Tupl.
 *
 * @author Brian S O'Neill
 */
public interface TuplPanicHandler {
    /**
     * Called when a Database panics.
     * 
     * @param database the affected database
     * @param type event type which triggered the panic
     * @param message event message
     * @param args additional event arguments
     */
    void onPanic(Database database, EventType type, String message, Object... args);
}
