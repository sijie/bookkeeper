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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * Unit test {@link StreamPositionImpl}.
 */
public class StreamPositionImplTest {

    private final long streamId;

    public StreamPositionImplTest() {
        this.streamId = System.currentTimeMillis();
    }

    @Test
    public void testUpdate() {
        StreamPositionImpl streamPos = new StreamPositionImpl(streamId);

        assertEquals(streamId, streamPos.getStreamId());

        long rangeId = 2 * System.currentTimeMillis();
        assertNull(streamPos.getPosition(rangeId));
        EventPositionImpl eventPos = EventPositionImpl.of(
            rangeId,
            3456L,
            7890L,
            123);
        streamPos.addPosition(eventPos);
        EventPositionImpl returnedEventPos = streamPos.getPosition(rangeId);
        assertSame(eventPos, returnedEventPos);
    }

}
