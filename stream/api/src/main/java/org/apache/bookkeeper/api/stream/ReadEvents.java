/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.api.stream;

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * A batch of events read from a stream.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ReadEvents<KeyT, ValueT> extends AutoCloseable {

    /**
     * Return the next event in this batch of events.
     * Return <tt>null</tt> if reach end of the batch.
     *
     * @return the next event in this batch of events, and return null
     *         if reach end of the batch.
     */
    ReadEvent<KeyT, ValueT> next();

    /**
     * Returns the num of events of this events batch.
     *
     * @return the num of events in this events batch.
     */
    int numEvents();

    /**
     * Return the estimated size of this events batch.
     *
     * @return the estimated size of this events batch.
     */
    int getEstimatedSize();

    /**
     * {@inheritDoc}
     */
    void close();
}
