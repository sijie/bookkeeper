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

package org.apache.bookkeeper.clients.impl.stream.event;

import org.apache.bookkeeper.api.stream.WriteEvent;
import org.apache.bookkeeper.api.stream.WriteEventBuilder;

/**
 * A default implementation of {@link WriteEventBuilder}.
 */
public class WriteEventBuilderImpl<KeyT, ValueT> implements WriteEventBuilder<KeyT, ValueT> {

    private final WriteEventImpl.Recycler<KeyT, ValueT> eventRecycler;

    private KeyT key;
    private ValueT value;
    private long timestamp = -1L;

    public WriteEventBuilderImpl() {
        this.eventRecycler = new WriteEventImpl.Recycler<>();
    }

    public WriteEventBuilderImpl(WriteEventImpl.Recycler<KeyT, ValueT> recycler) {
        this.eventRecycler = recycler;
    }

    @Override
    public WriteEventBuilder<KeyT, ValueT> withKey(KeyT key) {
        this.key = key;
        return this;
    }

    @Override
    public WriteEventBuilder<KeyT, ValueT> withValue(ValueT value) {
        this.value = value;
        return this;
    }

    @Override
    public WriteEventBuilder<KeyT, ValueT> withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return null;
    }

    @Override
    public WriteEvent<KeyT, ValueT> build() {
        WriteEventImpl<KeyT, ValueT> event = eventRecycler.newEvent();
        event.setKey(key);
        event.setValue(value);
        event.setTimestamp(timestamp);
        return event;
    }
}
