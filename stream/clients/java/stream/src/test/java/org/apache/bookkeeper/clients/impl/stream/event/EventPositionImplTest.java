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

import org.junit.Test;

/**
 * Unit test {@link EventPositionImpl}.
 */
public class EventPositionImplTest {

    @Test
    public void testConstructor() {
        long seed = System.currentTimeMillis();
        EventPositionImpl eventPos = EventPositionImpl.of(
            seed,
            2 * seed,
            3 * seed,
            1234);

        assertEquals(seed, eventPos.getRangeId());
        assertEquals(2 * seed, eventPos.getRangeOffset());
        assertEquals(3 * seed, eventPos.getRangeSeqNum());
        assertEquals(1234, eventPos.getSlotId());
    }

    @Test
    public void testCreateInitPos() {
        long seed = System.currentTimeMillis();
        EventPositionImpl eventPos = EventPositionImpl.createInitPos(seed);
        assertEquals(seed, eventPos.getRangeId());
        assertEquals(0L, eventPos.getRangeOffset());
        assertEquals(0L, eventPos.getRangeSeqNum());
        assertEquals(0, eventPos.getSlotId());
    }

}
