/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.api.stream;

import org.apache.bookkeeper.api.transaction.Transactional;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * Atomic write on a stream.
 *
 * <p>It provides the mechanism to write many events to a single stream atomically.
 *
 * <p>The transaction is unbounded in size but is bounded in time. If the transaction
 * is not committed or aborted within a time window specified at the time of its creation,
 * it will be automatically aborted.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface WriteBatch<KeyT, ValueT> extends Transactional {

    /**
     * Write an event to the stream in a transaction.
     *
     * <p>Events written in a transaction will not be visible to any readers before {@link #commit()} is called.
     *
     * @param event the event appended into the write batch.
     */
    void write(WriteEvent<KeyT, ValueT> event);

}
