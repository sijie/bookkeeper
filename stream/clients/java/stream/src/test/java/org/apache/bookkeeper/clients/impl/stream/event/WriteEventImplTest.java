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
import org.junit.Test;

/**
 * Unit test {@link WriteEventImpl}.
 */
public class WriteEventImplTest {

    private final WriteEventImpl.Recycler<ByteBuf, ByteBuf> recycler;

    public WriteEventImplTest() {
        this.recycler = new WriteEventImpl.Recycler<>();
    }

    @Test
    public void testNewEvent() {
        ByteBuf keyBuf = Unpooled.copiedBuffer("test-key", UTF_8);
        ByteBuf valBuf = Unpooled.copiedBuffer("test-value", UTF_8);

        WriteEventImpl<ByteBuf, ByteBuf> event = recycler.newEvent();
        assertNull(event.key());
        assertNull(event.value());
        assertEquals(-1L, event.timestamp());

        // set value
        event.setKey(keyBuf);
        event.setValue(valBuf);
        event.setTimestamp(System.currentTimeMillis());

        // close the event will release its resources
        event.close();
        assertNull(event.key());
        assertNull(event.value());
        assertEquals(-1L, event.timestamp());
        assertEquals(0, keyBuf.refCnt());
        assertEquals(0, valBuf.refCnt());

        // the event is put back to recycler queue. next get will return same event instance
        WriteEventImpl<ByteBuf, ByteBuf> newEvent = recycler.newEvent();
        assertSame(event, newEvent);
        assertNull(event.key());
        assertNull(event.value());
        assertEquals(-1L, event.timestamp());
    }

}
