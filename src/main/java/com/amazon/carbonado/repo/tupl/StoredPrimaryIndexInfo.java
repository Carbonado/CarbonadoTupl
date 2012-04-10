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

import com.amazon.carbonado.Independent;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.repo.indexed.StoredIndexInfo;

/**
 * Stores basic information about the primary indexes managed by
 * TuplRepository.
 *
 * <p>Note: This storable cannot have indexes defined, since it is used to
 * discover information about indexes. It would create a cyclic dependency.
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("indexName")
@Independent
public interface StoredPrimaryIndexInfo extends StoredIndexInfo {
    /** Evolution strategy code */
    public static final int EVOLUTION_NONE = 0, EVOLUTION_STANDARD = 1;

    /**
     * Returns the index name descriptor for the keys of this index. This
     * descriptor is defined by {@link com.amazon.carbonado.info.StorableIndex}, and
     * it does not contain type information.
     */
    @Nullable
    String getIndexNameDescriptor();
    void setIndexNameDescriptor(String descriptor);

    /**
     * Returns EVOLUTION_NONE if evolution of records is not supported.
     */
    int getEvolutionStrategy();
    void setEvolutionStrategy(int strategy);
}
