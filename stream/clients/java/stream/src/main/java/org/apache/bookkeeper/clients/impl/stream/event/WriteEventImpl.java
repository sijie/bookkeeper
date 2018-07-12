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

import io.netty.util.Recycler.Handle;
import org.apache.bookkeeper.api.stream.WriteEvent;

/**
 * The default implementation of {@link WriteEvent}.
 */
class WriteEventImpl<KeyT, ValueT>
        extends AbstractEventImpl<KeyT, ValueT>
        implements WriteEvent<KeyT, ValueT> {

    /**
     * A recycler to create recyclable write events.
     */
    static class Recycler<KeyT, ValueT>
        extends io.netty.util.Recycler<WriteEventImpl<KeyT, ValueT>> {

        @Override
        protected WriteEventImpl<KeyT, ValueT> newObject(Handle<WriteEventImpl<KeyT, ValueT>> handle) {
            return new WriteEventImpl<>(handle);
        }

        public WriteEventImpl<KeyT, ValueT> newEvent() {
            return get();
        }

    }

    private final Handle<WriteEventImpl<KeyT, ValueT>> handle;

    private WriteEventImpl(Handle<WriteEventImpl<KeyT, ValueT>> handle) {
        this.handle = handle;
        reset();
    }

    @Override
    public void close() {
        reset();
        handle.recycle(this);
    }
}
