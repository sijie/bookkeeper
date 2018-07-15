/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.impl.stream.range;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.api.stream.ReadEvent;
import org.apache.bookkeeper.api.stream.ReadEvents;
import org.apache.bookkeeper.clients.impl.stream.event.ReadEventsImpl;
import org.apache.bookkeeper.clients.impl.stream.event.ReadEventsImpl.Recycler;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.coder.VarIntCoder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.CompressionCodecType;
import org.apache.bookkeeper.stream.proto.EventSetType;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.protocol.EventSet;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link StreamRangeEventReaderImpl}.
 */
public class StreamRangeEventReaderImplTest {

    private final RangeId rid = RangeId.of(1234L, 5678L);
    private final RangePosition rangePos = RangePosition.newBuilder()
        .setRangeId(rid.getRangeId())
        .setOffset(1234L)
        .setSeqNum(2345L)
        .setSlotId(0)
        .build();
    private final RangeProperties rangeProps = RangeProperties.newBuilder()
        .setRangeId(rid.getRangeId())
        .setStorageContainerId(123L)
        .setStartHashKey(Long.MIN_VALUE)
        .setEndHashKey(Long.MAX_VALUE)
        .build();
    private final StreamRangeEventSetReaderImpl mockEventSetReader = mock(StreamRangeEventSetReaderImpl.class);
    private final StreamRangeClients mockRangeClients = mock(StreamRangeClients.class);
    private final EventSet.WriterBuilder<Integer, String> esWriterBuilder = EventSet.<Integer, String>newWriterBuilder()
        .withKeyCoder(VarIntCoder.of())
        .withValueCoder(StringUtf8Coder.of())
        .withBufferSize(8192)
        .withCompressionCodec(CompressionCodecType.NONE)
        .withEventSetType(EventSetType.DATA)
        .withMaxNumEvents(1024)
        .withWriterId(1234L);
    private final EventSet.ReaderBuilder<Integer, String> esReaderBuilder = EventSet.<Integer, String>newReaderBuilder()
        .withKeyCoder(VarIntCoder.of())
        .withValueCoder(StringUtf8Coder.of());
    private final ReadEventsImpl.Recycler<Integer, String> readEventsRecycler = new Recycler<>();
    private StreamRangeEventReaderImpl<Integer, String> reader;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() {
        final String streamName = "test-event-reader";
        executor = Executors.newSingleThreadScheduledExecutor();
        reader = new StreamRangeEventReaderImpl<>(
            mockRangeClients,
            streamName,
            rid,
            rangeProps,
            rangePos,
            esReaderBuilder,
            readEventsRecycler,
            executor);
        when(mockRangeClients.openStreamRangeReader(
            anyLong(),
            any(),
            any()
        )).thenReturn(FutureUtils.value(mockEventSetReader));
        when(mockEventSetReader.closeAsync()).thenReturn(FutureUtils.value(null));
    }

    @After
    public void tearDown() {
        if (null != reader) {
            reader.close();
        }
        if (null != executor) {
            executor.shutdown();
        }
    }

    @Test
    public void testInitialize() throws Exception {
        FutureUtils.result(reader.initialize());
        assertEquals(mockEventSetReader, reader.getEventSetReader());
    }

    @Test
    public void testClose() throws Exception {
        FutureUtils.result(reader.initialize());
        reader.close();
        verify(mockEventSetReader, times(1)).closeAsync();
    }

    @Test
    public void testCloseBeforeInitialization() {
        reader.close();
    }

    @Test
    public void testReadNext() throws Exception {
        FutureUtils.result(reader.initialize());
        EventSet.Writer<Integer, String> writer = esWriterBuilder.build();
        for (int i = 0; i < 10; i++) {
            writer.writeEvent(i, "event-" + i, System.currentTimeMillis());
        }
        ByteBuf data = writer.complete();
        int size = data.readableBytes();
        ReadData readData = ReadDataImpl.create(
            rid.getRangeId(),
            rangePos.getOffset(),
            rangePos.getSeqNum(),
            data);
        when(mockEventSetReader.readNext()).thenReturn(FutureUtils.value(readData));
        try (ReadEvents<Integer, String> readEvents = FutureUtils.result(reader.readNext())) {
            assertEquals(size, readEvents.getEstimatedSize());
            assertEquals(10, readEvents.numEvents());
            for (int i = 0; i < 10; i++) {
                try (ReadEvent<Integer, String> event = readEvents.next()) {
                    assertNotNull(event);
                    assertNotNull(event.key());
                    assertEquals(i, event.key().intValue());
                    assertEquals("event-" + i, event.value());
                }
            }
            assertEquals(1, data.refCnt());
        }
        assertEquals(0, data.refCnt());
    }

}
