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

package org.apache.bookkeeper.stream.storage.impl.stream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit test {@link OffsetSequencer}.
 */
public class OffsetSequencerTest {

    @Test
    public void testSequencer() {
        long startOffset = 12345L;
        OffsetSequencer sequencer = new OffsetSequencer(startOffset);
        assertEquals(startOffset, sequencer.currentOffset());
        long numBytes = 356L;
        assertEquals(startOffset + numBytes, sequencer.advanceOffset(numBytes));
        assertEquals(startOffset + numBytes, sequencer.currentOffset());
    }

    @Test
    public void startNegativeOffset() {
        OffsetSequencer sequencer = new OffsetSequencer(-1L);
        assertEquals(0L, sequencer.currentOffset());
        long numBytes = 356L;
        assertEquals(numBytes, sequencer.advanceOffset(numBytes));
        assertEquals(numBytes, sequencer.currentOffset());
    }

}
