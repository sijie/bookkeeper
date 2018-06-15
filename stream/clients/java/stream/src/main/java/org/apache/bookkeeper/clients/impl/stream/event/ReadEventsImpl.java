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
import io.netty.util.ReferenceCountUtil;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.bookkeeper.api.stream.ReadEvent;
import org.apache.bookkeeper.api.stream.ReadEvents;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * A default implementation of {@link ReadEvents}.
 */
@Getter(AccessLevel.PACKAGE)
public class ReadEventsImpl<KeyT, ValueT> implements ReadEvents<KeyT, ValueT> {

    /**
     * A recycler to create read events.
     */
    public static class Recycler<KeyT, ValueT>
        extends io.netty.util.Recycler<ReadEventsImpl<KeyT, ValueT>> {

        private final ReadEventImpl.Recycler<KeyT, ValueT> eventRecycler;

        public Recycler() {
            super();
            this.eventRecycler = new ReadEventImpl.Recycler<>();
        }

        @Override
        protected ReadEventsImpl<KeyT, ValueT> newObject(Handle<ReadEventsImpl<KeyT, ValueT>> handle) {
            return new ReadEventsImpl<>(handle, eventRecycler);
        }

        public ReadEventsImpl<KeyT, ValueT> create(String streamName,
                                                   RangeId rangeId,
                                                   EventSet.Reader<KeyT, ValueT> eventSetReader,
                                                   RangePositionImpl rangePos,
                                                   int estimatedSize) {
            ReadEventsImpl<KeyT, ValueT> events = get();
            events.streamName = streamName;
            events.rangeId = rangeId;
            events.reader = eventSetReader;
            events.rangePos = rangePos;
            events.slotId = 0;
            events.estimatedSize = estimatedSize;
            return events;
        }
    }

    private final Handle<ReadEventsImpl<KeyT, ValueT>> handle;
    private final ReadEventImpl.Recycler<KeyT, ValueT> eventRecycler;
    private String streamName;
    private RangeId rangeId;
    private EventSet.Reader<KeyT, ValueT> reader;
    private RangePositionImpl rangePos;
    private int slotId = 0;
    private int estimatedSize = 0;

    private ReadEventsImpl(Handle<ReadEventsImpl<KeyT, ValueT>> handle,
                           ReadEventImpl.Recycler<KeyT, ValueT> eventRecycler) {
        this.handle = handle;
        this.eventRecycler = eventRecycler;
        reset();
    }

    public EventPositionImpl getLastEventPosition() {
        return EventPositionImpl.of(
            rangePos.getRangeId(),
            rangePos.getRangeOffset(),
            rangePos.getRangeSeqNum(),
            reader.numEvents() - 1);
    }

    public RangeId getRangeId() {
        return rangeId;
    }

    private void reset() {
        streamName = null;
        ReferenceCountUtil.safeRelease(reader);
        reader = null;
        ReferenceCountUtil.safeRelease(rangePos);
        rangeId = null;
        rangePos = null;
        slotId = 0;
        estimatedSize = 0;
    }

    @Override
    public ReadEvent<KeyT, ValueT> next() {
        if (!reader.hasNext()) {
            return null;
        }
        reader.advance();

        ReadEventImpl<KeyT, ValueT> event = eventRecycler.newEvent();
        event.setName(streamName);
        event.setKey(retain(reader.getKey()));
        event.setValue(retain(reader.getValue()));
        Long timestamp = reader.getEventTime();
        event.setTimestamp(null == timestamp ? -1L : timestamp);
        event.setPosition(EventPositionImpl.of(
            rangePos.getRangeId(),
            rangePos.getRangeOffset(),
            rangePos.getRangeSeqNum(),
            slotId++
        ));
        return event;
    }

    @Override
    public int getEstimatedSize() {
        return estimatedSize;
    }

    @Override
    public int numEvents() {
        return reader.numEvents();
    }

    @Override
    public void close() {
        reset();
        handle.recycle(this);
    }
}
