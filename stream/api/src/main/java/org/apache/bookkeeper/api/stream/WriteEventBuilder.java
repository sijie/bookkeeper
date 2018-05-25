/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.api.stream;

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * Builder to build write events.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface WriteEventBuilder<KeyT, ValueT> {

    /**
     * Build a write event with a provided <tt>key</tt>.
     *
     * @return write event builder
     */
    WriteEventBuilder<KeyT, ValueT> withKey(KeyT key);

    /**
     * Build a write event with a provided <tt>value</tt>.
     *
     * @return write event builder
     */
    WriteEventBuilder<KeyT, ValueT> withValue(ValueT value);

    /**
     * Build a write event timestamp.
     *
     * @param timestamp event timestamp
     * @return write event builder
     */
    WriteEventBuilder<KeyT, ValueT> withTimestamp(long timestamp);

    /**
     * Build the event.
     *
     * @return the built event.
     */
    WriteEvent<KeyT, ValueT> build();

}
