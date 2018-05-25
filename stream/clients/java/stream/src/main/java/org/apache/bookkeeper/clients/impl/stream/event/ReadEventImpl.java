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
import lombok.AccessLevel;
import lombok.Setter;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.ReadEvent;

/**
 * The default implementation of {@link ReadEvent}.
 */
@Setter(AccessLevel.PACKAGE)
class ReadEventImpl<KeyT, ValueT>
        extends AbstractEventImpl<KeyT, ValueT>
        implements ReadEvent<KeyT, ValueT> {

    static class Recycler<KeyT, ValueT>
        extends io.netty.util.Recycler<ReadEventImpl<KeyT, ValueT>> {

        @Override
        protected ReadEventImpl<KeyT, ValueT> newObject(Handle<ReadEventImpl<KeyT, ValueT>> handle) {
            return new ReadEventImpl<>(handle);
        }

        public ReadEventImpl<KeyT, ValueT> newEvent() {
            return get();
        }

    }

    private final Handle<ReadEventImpl<KeyT, ValueT>> handle;

    private String name;
    private Position position;

    private ReadEventImpl(Handle<ReadEventImpl<KeyT, ValueT>> handle) {
        this.handle = handle;
        reset();
    }

    @Override
    protected void reset() {
        super.reset();
        name = null;
        position = null;
    }

    @Override
    public String stream() {
        return name;
    }

    @Override
    public Position position() {
        return position;
    }

    @Override
    public void close() {
        reset();
        if (null != handle) {
            handle.recycle(this);
        }
    }
}
