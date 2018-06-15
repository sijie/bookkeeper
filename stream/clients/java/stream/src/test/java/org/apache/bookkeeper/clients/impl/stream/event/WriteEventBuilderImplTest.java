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
import org.apache.bookkeeper.api.stream.WriteEvent;
import org.junit.Test;

/**
 * Unit test {@link WriteEventBuilderImpl}.
 */
public class WriteEventBuilderImplTest {

    private final WriteEventBuilderImpl<ByteBuf, ByteBuf> writeEventBuilder;

    public WriteEventBuilderImplTest() {
        this.writeEventBuilder = new WriteEventBuilderImpl<>();
    }

    @Test
    public void testBuild() {
        ByteBuf keyBuf = Unpooled.copiedBuffer("test-key", UTF_8);
        ByteBuf valBuf = Unpooled.copiedBuffer("test-value", UTF_8);
        long timestamp = System.currentTimeMillis();

        WriteEvent<ByteBuf, ByteBuf> event = writeEventBuilder
            .withKey(keyBuf)
            .withValue(valBuf)
            .withTimestamp(timestamp)
            .build();
        assertSame(keyBuf, event.key());
        assertSame(valBuf, event.value());
        assertEquals(timestamp, event.timestamp());

        event.close();
        assertNull(event.key());
        assertNull(event.value());
        assertEquals(-1L, event.timestamp());
    }

}
