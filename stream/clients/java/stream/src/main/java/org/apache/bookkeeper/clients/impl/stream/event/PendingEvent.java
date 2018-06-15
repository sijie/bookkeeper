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

import static io.netty.util.ReferenceCountUtil.retain;

import io.netty.util.Recycler.Handle;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.WriteEvent;
import org.apache.bookkeeper.api.stream.WriteEventBuilder;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamRuntimeException;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * A pending event to a given stream.
 */
@Getter
public class PendingEvent<KeyT, ValueT> implements AutoCloseable {

    public static <KeyT, ValueT> Iterator<PendingEvent<KeyT, ValueT>> iterate(
        EventSet.ReaderBuilder<KeyT, ValueT> esReaderBuilder,
        PendingEventSet eventSet,
        WriteEventBuilder<KeyT, ValueT> eventBuilder,
        PendingEvent.Recycler<KeyT, ValueT> pendingEventRecycler) {
        EventSet.Reader<KeyT, ValueT> reader;
        try {
            reader = esReaderBuilder.build(eventSet.getEventSetBuf());
        } catch (IOException e) {
            throw new InternalStreamRuntimeException(StatusCode.INVALID_EVENT_SET, e);
        }
        return new Iterator<PendingEvent<KeyT, ValueT>>() {

            int idx = 0;

            @Override
            public boolean hasNext() {
                return reader.hasNext();
            }

            @Override
            public PendingEvent<KeyT, ValueT> next() {
                reader.advance();

                Long eventTime = reader.getEventTime();
                WriteEvent<KeyT, ValueT> writeEvent = eventBuilder
                    .withKey(retain(reader.getKey()))
                    .withValue(retain(reader.getValue()))
                    .withTimestamp(null == eventTime ? -1L : eventTime)
                    .build();
                PendingEvent<KeyT, ValueT> pendingEvent = pendingEventRecycler.newEvent(
                    writeEvent,
                    eventSet.getCallbacks().get(idx)
                );

                // move the next event
                ++idx;
                return pendingEvent;
            }
        };
    }

    /**
     * A recycler to create recyclable write events.
     */
    public static class Recycler<KeyT, ValueT>
        extends io.netty.util.Recycler<PendingEvent<KeyT, ValueT>> {

        @Override
        protected PendingEvent<KeyT, ValueT> newObject(Handle<PendingEvent<KeyT, ValueT>> handle) {
            return new PendingEvent<>(handle);
        }

        public PendingEvent<KeyT, ValueT> newEvent(WriteEvent<KeyT, ValueT> event,
                                                   CompletableFuture<Position> future) {
            PendingEvent<KeyT, ValueT> pendingEvent = get();
            pendingEvent.event = event;
            pendingEvent.future = future;
            return pendingEvent;
        }

    }

    private final Handle<PendingEvent<KeyT, ValueT>> handle;
    private WriteEvent<KeyT, ValueT> event;
    private CompletableFuture<Position> future;

    private PendingEvent(Handle<PendingEvent<KeyT, ValueT>> handle) {
        this.handle = handle;
        reset();
    }

    private void reset() {
        if (null != event) {
            event.close();
            event = null;
        }
        future = null;
    }

    public WriteEvent<KeyT, ValueT> getAndReset() {
        WriteEvent<KeyT, ValueT> savedEvent = event;
        event = null;
        return savedEvent;
    }

    @Override
    public void close() {
        reset();
        handle.recycle(this);
    }

}
