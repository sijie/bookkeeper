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

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_OFFSET;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_RANGE_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_SEQ_NUM;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit test {@link RangePositionImpl}.
 */
public class RangePositionImplTest {

    @Test
    public void testRelease() {
        long seed = System.currentTimeMillis();

        RangePositionImpl pos = RangePositionImpl.create(seed, seed * 2 , seed * 3);
        assertEquals(1, pos.refCnt());
        assertEquals(seed, pos.getRangeId());
        assertEquals(2 * seed, pos.getRangeOffset());
        assertEquals(3 * seed, pos.getRangeSeqNum());

        // release the pos object will recycle it
        pos.release();

        assertEquals(INVALID_RANGE_ID, pos.getRangeId());
        assertEquals(INVALID_OFFSET, pos.getRangeOffset());
        assertEquals(INVALID_SEQ_NUM, pos.getRangeSeqNum());
    }

}
