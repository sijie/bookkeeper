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
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bookkeeper.api.stream.ReadEvent;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet.Writer;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.coder.VarIntCoder;
import org.apache.bookkeeper.stream.proto.CompressionCodecType;
import org.apache.bookkeeper.stream.proto.EventSetType;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test {@link ReadEventsImpl}.
 */
public class ReadEventsImplTest {

    private static ByteBuf newEventSet(int numEvents) {
        Writer<Integer, String> eventSetWriter = EventSet.<Integer, String>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(StringUtf8Coder.of())
            .withCompressionCodec(CompressionCodecType.NONE)
            .withEventSetType(EventSetType.DATA)
            .withMaxNumEvents(1024)
            .withBufferSize(512 * 1024)
            .withWriterId(0)
            .withIdempotency(false)
            .build();

        for (int i = 0; i < numEvents; i++) {
            assertTrue(eventSetWriter.writeEvent(
                i,
                String.format("event-%03d", i),
                10000L + i));
        }
        return eventSetWriter.complete();
    }

    private static EventSet.Reader<Integer, String> newEventSetReader(ByteBuf data) throws IOException {
        return EventSet.<Integer, String>newReaderBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(StringUtf8Coder.of())
            .build(data);
    }

    @Rule
    public final TestName runtime = new TestName();

    private final ReadEventsImpl.Recycler<Integer, String> readEventsRecycler;

    public ReadEventsImplTest() {
        this.readEventsRecycler = new ReadEventsImpl.Recycler<>();
    }

    @Test
    public void testNewReadEvent() throws Exception {
        int numEvents = ThreadLocalRandom.current().nextInt(128, 512);
        ByteBuf eventSetBuf = newEventSet(numEvents);
        int numBytes = eventSetBuf.readableBytes();
        EventSet.Reader<Integer, String> eventSetReader = newEventSetReader(eventSetBuf);

        final String streamName = runtime.getMethodName();
        final long streamId = System.currentTimeMillis();
        final long rangeId = System.currentTimeMillis() * 2;
        final RangeId rid = RangeId.of(streamId, rangeId);
        final long offset = System.currentTimeMillis() / 2;
        final long seqNum = System.currentTimeMillis() * 3;
        final RangePositionImpl rangePos = RangePositionImpl.create(
            rangeId,
            offset,
            seqNum);

        ReadEventsImpl<Integer, String> readEvents = readEventsRecycler.create(
            streamName,
            rid,
            eventSetReader,
            rangePos,
            numBytes
        );
        assertEquals(rid, readEvents.getRangeId());
        assertEquals(EventPositionImpl.of(
            rangeId,
            offset,
            seqNum,
            numEvents - 1
        ), readEvents.getLastEventPosition());
        assertEquals(numBytes, readEvents.getEstimatedSize());
        assertEquals(numEvents, readEvents.numEvents());

        int numReadEvents = 0;
        ReadEvent<Integer, String> readEvent = null;
        while ((readEvent = readEvents.next()) != null) {
            assertEquals(numReadEvents, readEvent.key().intValue());
            assertEquals(String.format("event-%03d", numReadEvents), readEvent.value());
            assertEquals(streamName, readEvent.stream());
            assertEquals(10000L + numReadEvents, readEvent.timestamp());
            assertEquals(
                EventPositionImpl.of(
                    rangeId,
                    offset,
                    seqNum,
                    numReadEvents
                ),
                readEvent.position());

            ++numReadEvents;
            readEvent.close();
        }
        assertEquals(numEvents, numReadEvents);
        assertEquals(numEvents, readEvents.getSlotId());
        assertEquals(numBytes, readEvents.getEstimatedSize());
        readEvents.close();

        // after `ReadEventsImpl#close`, the `rangePos` is recycled.
        assertEquals(0, rangePos.refCnt());
        // after `ReadEventsImpl#close`, the object will be recycled and put back to the object pool
        assertNull(readEvents.getStreamName());
        assertNull(readEvents.getRangeId());
        assertNull(readEvents.getReader());
        assertNull(readEvents.getRangePos());
        assertEquals(0, readEvents.getSlotId());
        assertEquals(0, readEvents.getEstimatedSize());
    }

}
