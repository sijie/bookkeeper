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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.WriteEvent;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.junit.Test;

/**
 * Unit test {@link PendingEvent}.
 */
public class PendingEventTest {

    private final WriteEventBuilderImpl<ByteBuf, ByteBuf> writeEventBuilder;
    private final PendingEvent.Recycler<ByteBuf, ByteBuf> recycler;

    public PendingEventTest() {
        this.writeEventBuilder = new WriteEventBuilderImpl<>();
        this.recycler = new PendingEvent.Recycler<>();
    }

    @Test
    public void testNewEvent() {
        ByteBuf keyBuf = Unpooled.copiedBuffer("test-key", UTF_8);
        ByteBuf valBuf = Unpooled.copiedBuffer("test-value", UTF_8);
        long timestamp = System.currentTimeMillis();

        WriteEvent<ByteBuf, ByteBuf> writeEvent = writeEventBuilder
            .withKey(keyBuf)
            .withValue(valBuf)
            .withTimestamp(timestamp)
            .build();
        CompletableFuture<Position> writeFuture = FutureUtils.createFuture();

        PendingEvent<ByteBuf, ByteBuf> pendingEvent = recycler.newEvent(writeEvent, writeFuture);
        assertSame(writeEvent, pendingEvent.getEvent());
        assertSame(writeFuture, pendingEvent.getFuture());

        // close pending event should release write event and then corresponding key/value pair
        pendingEvent.close();
        assertNull(pendingEvent.getEvent());
        assertNull(pendingEvent.getFuture());
        assertNull(writeEvent.key());
        assertNull(writeEvent.value());
        assertEquals(-1L, writeEvent.timestamp());
        assertEquals(0, keyBuf.refCnt());
        assertEquals(0, valBuf.refCnt());

        // the pending event object should be put back to the recycler queue.
        PendingEvent<ByteBuf, ByteBuf> newPendingEvent = recycler.get();
        assertSame(pendingEvent, newPendingEvent);
        newPendingEvent.close();
    }

    @Test
    public void testGetAndReset() {
        ByteBuf keyBuf = Unpooled.copiedBuffer("test-key", UTF_8);
        ByteBuf valBuf = Unpooled.copiedBuffer("test-value", UTF_8);
        long timestamp = System.currentTimeMillis();

        WriteEvent<ByteBuf, ByteBuf> writeEvent = writeEventBuilder
            .withKey(keyBuf)
            .withValue(valBuf)
            .withTimestamp(timestamp)
            .build();
        CompletableFuture<Position> writeFuture = FutureUtils.createFuture();

        PendingEvent<ByteBuf, ByteBuf> pendingEvent = recycler.newEvent(writeEvent, writeFuture);
        assertSame(writeEvent, pendingEvent.getEvent());
        assertSame(writeFuture, pendingEvent.getFuture());

        WriteEvent<ByteBuf, ByteBuf> retrievedWriteEvent = pendingEvent.getAndReset();
        assertSame(retrievedWriteEvent, writeEvent);
        assertNull(pendingEvent.getEvent());
        assertSame(writeFuture, pendingEvent.getFuture());

        // close the pending event will only reset its state but will not impact WriteEvent
        pendingEvent.close();
        assertNull(pendingEvent.getEvent());
        assertNull(pendingEvent.getFuture());
        assertSame(keyBuf, writeEvent.key());
        assertSame(valBuf, writeEvent.value());
        assertEquals(timestamp, writeEvent.timestamp());

        // close the write event
        writeEvent.close();
    }

}
