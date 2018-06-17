/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.impl.stream;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.WriteEvent;
import org.apache.bookkeeper.api.stream.WriteEventBuilder;
import org.apache.bookkeeper.api.stream.WriterConfig;
import org.apache.bookkeeper.clients.impl.internal.api.HashStreamRanges;
import org.apache.bookkeeper.clients.impl.internal.api.MetaRangeClient;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.impl.stream.event.EventPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.event.PendingWrite;
import org.apache.bookkeeper.clients.impl.stream.event.WriteEventBuilderImpl;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeEventWriter;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeEventWriterFactory;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.coder.VarIntCoder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.router.HashRouter;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.RangeKeyType;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.protocol.util.ProtoUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test for {@link WriterImpl}.
 */
@Slf4j
public class WriterImplTest {

    private static final long streamId = 12345L;

    @Rule
    public TestName runtime = new TestName();

    private final HashStreamRanges streamRanges1 = prepareRanges(streamId, 4, 0);
    private final HashStreamRanges streamRanges2 = prepareRanges(streamId, 8, 4L);
    private final HashStreamRanges streamRanges3 = prepareRanges(streamId, 80, 12L);
    private final HashRouter<Integer> router = new HashRouter<Integer>() {

        private static final long serialVersionUID = -9119055960554608491L;

        private final List<Long> keys = Lists.newArrayList(streamRanges3.getRanges().keySet());

        @Override
        public Long getRoutingKey(Integer key) {
            int idx;
            if (null == key) {
                idx = ThreadLocalRandom.current().nextInt(keys.size());
            } else {
                idx = key % keys.size();
            }
            return keys.get(idx);
        }
    };
    private final StreamProperties streamProps = StreamProperties.newBuilder()
        .setStorageContainerId(12345L)
        .setStreamConf(DEFAULT_STREAM_CONF)
        .setStreamId(streamId)
        .setStreamName("test-stream")
        .build();
    private final MetaRangeClient mockMetaRangeClient = mock(MetaRangeClient.class);
    private final StorageServerClientManager mockClientManager = mock(StorageServerClientManager.class);

    private final StreamConfig<Integer, String> streamConfig = StreamConfig.<Integer, String>builder()
        .keyCoder(VarIntCoder.of())
        .valueCoder(StringUtf8Coder.of())
        .keyRouter(router)
        .build();
    private final WriterConfig writerConfig = WriterConfig.builder().build();
    private final WriteEventBuilder<Integer, String> writeEventBuilder = new WriteEventBuilderImpl<>();
    private OrderedScheduler scheduler;

    private static HashStreamRanges prepareRanges(long streamId, int numRanges, long nextRangeId) {
        List<RangeProperties> ranges = ProtoUtils.split(streamId, numRanges, nextRangeId, (sid, rid) -> 1L);
        NavigableMap<Long, RangeProperties> rangeMap = Maps.newTreeMap();
        for (RangeProperties props : ranges) {
            rangeMap.put(props.getStartHashKey(), props);
        }
        return HashStreamRanges.ofHash(
            RangeKeyType.HASH,
            rangeMap);
    }

    @Before
    public void setUp() {
        when(mockClientManager.openMetaRangeClient(any(StreamProperties.class)))
            .thenReturn(mockMetaRangeClient);
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .numThreads(1)
            .name("test-scheduler")
            .build();
    }

    private WriterImpl<Integer, String> createWriter() {
        return new WriterImpl<>(
            runtime.getMethodName(),
            runtime.getMethodName(),
            streamProps,
            streamConfig,
            writerConfig,
            mockClientManager,
            scheduler);
    }

    @SuppressWarnings("unchecked")
    private WriterImpl<Integer, String> createWriter(
            Function<RangeProperties, StreamRangeEventWriter<Integer, String>> writerFactory) {
        return new WriterImpl<>(
            runtime.getMethodName(),
            runtime.getMethodName(),
            streamProps,
            streamConfig,
            writerConfig,
            mockClientManager,
            scheduler,
            new StreamRangeEventWriterFactory() {
                @Override
                public <KeyT, ValueT> StreamRangeEventWriter<KeyT, ValueT> createRangeEventWriter(
                    String writerName,
                    StreamConfig<KeyT, ValueT> streamConfig,
                    StreamProperties streamProps,
                    RangeProperties rangeProps,
                    WriterConfig writerConfig) {
                    return (StreamRangeEventWriter<KeyT, ValueT>) writerFactory.apply(rangeProps);
                }
            });
    }

    @Test
    public void testInitializeFailureOnGetActiveRanges() {
        IOException cause = new IOException("test-cause");
        when(mockMetaRangeClient.getActiveDataRanges())
            .thenReturn(FutureUtils.exception(cause));

        WriterImpl<Integer, String> writer = createWriter();
        try {
            FutureUtils.result(writer.initialize());
            fail("Should fail initializing the writer with exception " + cause);
        } catch (Exception e) {
            assertEquals(cause, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBasicFlow() throws Exception {
        when(mockMetaRangeClient.getActiveDataRanges())
            .thenReturn(FutureUtils.value(streamRanges1));

        ConcurrentMap<Long, StreamRangeEventWriter<Integer, String>> writers = Maps.newConcurrentMap();
        Function<RangeProperties, StreamRangeEventWriter<Integer, String>> writerFactory = rangeProperties -> {
            StreamRangeEventWriter<Integer, String> writer = writers.get(rangeProperties.getRangeId());
            if (null != writer) {
                return writer;
            }

            final AtomicLong sequencer = new AtomicLong(0L);
            writer = mock(StreamRangeEventWriter.class);
            try {
                doAnswer(invocation -> {
                    WriteEvent<Integer, String> writeEvent = invocation.getArgument(0);
                    CompletableFuture<Position> writeFuture = invocation.getArgument(1);
                    long sequenceId = sequencer.getAndIncrement();
                    writeFuture.complete(EventPositionImpl.of(
                        rangeProperties.getRangeId(),
                        sequenceId,
                        sequenceId,
                        0));
                    writeEvent.close();
                    return null;
                }).when(writer).write(any(WriteEvent.class), any(CompletableFuture.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            writers.put(rangeProperties.getRangeId(), writer);
            return writer;
        };

        WriterImpl<Integer, String> writer = createWriter(writerFactory);
        assertEquals(0, writer.getActiveWriters().size());
        assertNull(writer.getRangeRouter().getRanges());

        // initialize the writer
        assertSame(writer, FutureUtils.result(writer.initialize()));
        assertNotNull(writer.getRangeRouter().getRanges());
        assertEquals(streamRanges1, writer.getRangeRouter().getRanges());
        assertEquals(4, writers.size());
        assertEquals(4, writer.getActiveWriters().size());
        assertEquals(4, writer.getRangeRouter().getRanges().getRanges().size());
        assertTrue(Maps.difference(writer.getActiveWriters(), writers).areEqual());

        log.info("Range router is - {}.", writer.getRangeRouter().getRanges().getRanges());

        // write 80 messages
        int numEvents = 80;
        List<CompletableFuture<Position>> futures = writeNumEvents(writer, numEvents);
        List<Position> writeResults = FutureUtils.result(FutureUtils.collect(futures));
        assertEquals(numEvents, writeResults.size());

        // verify the calls
        List<Long> keys80 = Lists.newArrayList(streamRanges3.getRanges().keySet());
        List<Long> keys4 = Lists.newArrayList(streamRanges1.getRanges().keySet());
        Map<Long, List<Long>> groupedKeys =
            keys80.stream().collect(Collectors.groupingBy(key80 -> {
                long lastKey = Long.MIN_VALUE;
                for (Long key4 : keys4) {
                    if (key4 > key80) {
                        break;
                    }
                    lastKey = key4;
                }
                return lastKey;
            }));
        Map<Long, List<Long>> sortedMap = Maps.newTreeMap();
        sortedMap.putAll(groupedKeys);
        List<Integer> keys4Count = Lists.transform(
            Lists.newArrayList(sortedMap.values()),
            List::size);

        int idx = 0;
        for (StreamRangeEventWriter<Integer, String> drWriter : writers.values()) {
            log.info("Verifying writer {}", idx);
            verify(drWriter, times(keys4Count.get(idx)))
                .write(any(WriteEvent.class), any(CompletableFuture.class));

            when(drWriter.flush()).thenReturn(FutureUtils.value(null));
            log.info("Verified writer {}", idx);
            ++idx;
        }

        FutureUtils.result(writer.flush());
        for (StreamRangeEventWriter<Integer, String> drWriter : writers.values()) {
            verify(drWriter, times(1)).flush();
        }

        writer.close();
        for (StreamRangeEventWriter<Integer, String> drWriter : writers.values()) {
            verify(drWriter, times(2)).flush();
        }

        // double close
        writer.close();
        for (StreamRangeEventWriter<Integer, String> drWriter : writers.values()) {
            verify(drWriter, times(2)).flush();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUpdateLatestDataRanges() throws Exception {
        int numPendingsOnClose = 10;

        when(mockMetaRangeClient.getActiveDataRanges())
            .thenReturn(FutureUtils.value(streamRanges1))
            .thenReturn(FutureUtils.value(streamRanges2))
            .thenReturn(FutureUtils.value(streamRanges3));

        ConcurrentMap<Long, StreamRangeEventWriter<Integer, String>> writers1 = Maps.newConcurrentMap();
        ConcurrentMap<Long, StreamRangeEventWriter<Integer, String>> writers2 = Maps.newConcurrentMap();
        ConcurrentMap<Long, StreamRangeEventWriter<Integer, String>> writers3 = Maps.newConcurrentMap();

        Function<RangeProperties, StreamRangeEventWriter<Integer, String>> writerFactory = rangeProperties -> {
            StreamRangeEventWriter<Integer, String> writer;
            if (rangeProperties.getRangeId() < 4L) {
                writer = writers1.get(rangeProperties.getRangeId());
            } else if (rangeProperties.getRangeId() < 12L) {
                writer = writers2.get(rangeProperties.getRangeId());
            } else {
                writer = writers3.get(rangeProperties.getRangeId());
            }
            if (null != writer) {
                return writer;
            }

            final AtomicLong sequencer = new AtomicLong(0L);
            writer = mock(StreamRangeEventWriter.class);
            try {
                doAnswer(invocation -> {
                    CompletableFuture<Position> writeFuture = invocation.getArgument(3);
                    long sequence = sequencer.getAndIncrement();
                    writeFuture.complete(EventPositionImpl.of(
                        rangeProperties.getRangeId(),
                        sequence,
                        sequence,
                        0));
                    return null;
                }).when(writer).write(any(WriteEvent.class), any(CompletableFuture.class));
                when(writer.closeAndGetPendingWrites())
                    .thenReturn(((Supplier<List<PendingWrite>>) () -> {
                        List<PendingWrite> pendingWrites = Lists.newArrayListWithExpectedSize(numPendingsOnClose);
                        for (int i = 0; i < numPendingsOnClose; i++) {
                            pendingWrites.add(mock(PendingWrite.class));
                        }
                        return pendingWrites;
                    }).get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (rangeProperties.getRangeId() < 4L) {
                writers1.put(rangeProperties.getRangeId(), writer);
            } else if (rangeProperties.getRangeId() < 12L) {
                writers2.put(rangeProperties.getRangeId(), writer);
            } else {
                writers3.put(rangeProperties.getRangeId(), writer);
            }
            return writer;
        };

        WriterImpl<Integer, String> writer = createWriter(writerFactory);
        assertEquals(0, writer.getActiveWriters().size());
        assertNull(writer.getRangeRouter().getRanges());

        // initialize the writer
        assertSame(writer, FutureUtils.result(writer.initialize()));
        assertNotNull(writer.getRangeRouter().getRanges());
        assertEquals(streamRanges1, writer.getRangeRouter().getRanges());
        assertEquals(4, writers1.size());
        assertEquals(0, writers2.size());
        assertEquals(0, writers3.size());
        assertEquals(4, writer.getActiveWriters().size());
        assertEquals(4, writer.getRangeRouter().getRanges().getRanges().size());
        assertTrue(Maps.difference(writer.getActiveWriters(), writers1).areEqual());

        log.info("Range router is - {}.", writer.getRangeRouter().getRanges().getRanges());

        // refresh the data ranges to 8 ranges
        List<PendingWrite> pendings = FutureUtils.result(writer.updateLatestStreamRanges());
        assertEquals(streamRanges2, writer.getRangeRouter().getRanges());
        assertEquals(4, writers1.size());
        assertEquals(8, writers2.size());
        assertEquals(0, writers3.size());
        assertEquals(8, writer.getActiveWriters().size());
        assertEquals(8, writer.getRangeRouter().getRanges().getRanges().size());
        assertTrue(Maps.difference(writer.getActiveWriters(), writers2).areEqual());

        for (StreamRangeEventWriter<Integer, String> w : writers1.values()) {
            verify(w, times(1)).closeAndGetPendingWrites();
        }
        assertEquals(4 * numPendingsOnClose, pendings.size());

        // refresh the data ranges to 80 ranges
        pendings = FutureUtils.result(writer.updateLatestStreamRanges());
        assertEquals(streamRanges3, writer.getRangeRouter().getRanges());
        assertEquals(4, writers1.size());
        assertEquals(8, writers2.size());
        assertEquals(80, writers3.size());
        assertEquals(80, writer.getActiveWriters().size());
        assertEquals(80, writer.getRangeRouter().getRanges().getRanges().size());
        assertTrue(Maps.difference(writer.getActiveWriters(), writers3).areEqual());

        for (StreamRangeEventWriter<Integer, String> w : writers2.values()) {
            verify(w, times(1)).closeAndGetPendingWrites();
        }
        assertEquals(8 * numPendingsOnClose, pendings.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWriteEventsAfterClosed() throws Exception {
        HashStreamRanges streamRanges = prepareRanges(streamId, 2, 0L);
        when(mockMetaRangeClient.getActiveDataRanges())
            .thenReturn(FutureUtils.value(streamRanges));

        ConcurrentMap<Long, StreamRangeEventWriter<Integer, String>> writers = Maps.newConcurrentMap();
        Function<RangeProperties, StreamRangeEventWriter<Integer, String>> writerFactory = rangeProperties -> {
            StreamRangeEventWriter<Integer, String> writer = writers.get(rangeProperties.getRangeId());
            if (null != writer) {
                return writer;
            }
            writer = mock(StreamRangeEventWriter.class);
            try {
                doAnswer(invocation -> {
                    CompletableFuture<Position> writeFuture = invocation.getArgument(1);
                    writeFuture.completeExceptionally(
                        new InternalStreamException(StatusCode.WRITE_CANCELLED, "writes are cancelled"));
                    return null;
                }).when(writer).write(any(WriteEvent.class), any(CompletableFuture.class));
                when(writer.flush()).thenReturn(FutureUtils.value(null));
                when(writer.closeAsync()).thenReturn(FutureUtils.value(null));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            writers.put(rangeProperties.getRangeId(), writer);
            return writer;
        };

        WriterImpl<Integer, String> writer = createWriter(writerFactory);
        assertEquals(0, writer.getActiveWriters().size());
        assertNull(writer.getRangeRouter().getRanges());

        // initialize the writer
        assertSame(writer, FutureUtils.result(writer.initialize()));
        assertNotNull(writer.getRangeRouter().getRanges());
        assertEquals(streamRanges, writer.getRangeRouter().getRanges());
        assertEquals(2, writers.size());
        assertEquals(2, writer.getActiveWriters().size());
        assertEquals(2, writer.getRangeRouter().getRanges().getRanges().size());
        assertTrue(Maps.difference(writer.getActiveWriters(), writers).areEqual());

        log.info("Range router is - {}.", writer.getRangeRouter().getRanges().getRanges());

        writer.close();
        for (StreamRangeEventWriter<Integer, String> w : writers.values()) {
            verify(w, times(1)).flush();
            verify(w, times(1)).closeAsync();
        }

        CompletableFuture<Position> writeFuture = FutureUtils.createFuture();
        WriteEvent<Integer, String> writeEvent = writeEventBuilder
            .withKey(1)
            .withValue("event-1")
            .withTimestamp(System.currentTimeMillis())
            .build();
        writer.write(writeEvent, writeFuture);
        try {
            FutureUtils.result(writeFuture);
            fail("the write should be rejected when data range writers are closed");
        } catch (InternalStreamException e) {
            // expected
            assertEquals(StatusCode.WRITE_CANCELLED, e.getCode());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHandleDataRangeFenced() throws Exception {
        when(mockMetaRangeClient.getActiveDataRanges())
            .thenReturn(FutureUtils.value(streamRanges1))
            .thenReturn(FutureUtils.value(streamRanges2));

        ConcurrentMap<Long, StreamRangeEventWriter<Integer, String>> writers1 = Maps.newConcurrentMap();
        ConcurrentMap<Long, StreamRangeEventWriter<Integer, String>> writers2 = Maps.newConcurrentMap();

        Function<RangeProperties, StreamRangeEventWriter<Integer, String>> writerFactory = rangeProperties -> {
            StreamRangeEventWriter<Integer, String> writer;
            if (rangeProperties.getRangeId() < 4L) {
                writer = writers1.get(rangeProperties.getRangeId());
            } else {
                writer = writers2.get(rangeProperties.getRangeId());
            }
            if (null != writer) {
                return writer;
            }

            final AtomicLong sequencer = new AtomicLong(0L);
            writer = mock(StreamRangeEventWriter.class);
            try {
                doAnswer(invocation -> {
                    CompletableFuture<Position> writeFuture = invocation.getArgument(1);
                    long sequence = sequencer.getAndIncrement();
                    writeFuture.complete(EventPositionImpl.of(
                        rangeProperties.getRangeId(),
                        sequence,
                        sequence,
                        0));
                    if (rangeProperties.getRangeId() < 4L) {
                        throw new InternalStreamException(
                            StatusCode.STREAM_RANGE_FENCED,
                            "Range " + rangeProperties.getRangeId() + " is fenced");
                    }
                    return null;
                }).when(writer).write(any(WriteEvent.class), any(CompletableFuture.class));
                when(writer.closeAndGetPendingWrites())
                    .thenReturn(Lists.newArrayList());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            log.info("Created writer for data range {}.", rangeProperties.getRangeId());
            if (rangeProperties.getRangeId() < 4L) {
                writers1.put(rangeProperties.getRangeId(), writer);
            } else {
                writers2.put(rangeProperties.getRangeId(), writer);
            }
            return writer;
        };

        WriterImpl<Integer, String> writer = createWriter(writerFactory);
        assertEquals(0, writer.getActiveWriters().size());
        assertNull(writer.getRangeRouter().getRanges());

        // initialize the writer
        assertSame(writer, FutureUtils.result(writer.initialize()));
        assertNotNull(writer.getRangeRouter().getRanges());
        assertEquals(streamRanges1, writer.getRangeRouter().getRanges());
        assertEquals(4, writers1.size());
        assertEquals(4, writer.getActiveWriters().size());
        assertEquals(4, writer.getRangeRouter().getRanges().getRanges().size());
        assertTrue(Maps.difference(writer.getActiveWriters(), writers1).areEqual());

        log.info("Range router is - {}.", writer.getRangeRouter().getRanges().getRanges());

        // write 80 messages
        int numEvents = 80;
        List<CompletableFuture<Position>> futures = writeNumEvents(writer, numEvents);
        List<Position> writeResults = FutureUtils.result(FutureUtils.collect(futures));
        assertEquals(numEvents, writeResults.size());
        log.info("Completed writing {} events.", numEvents);

        // verify the writer
        assertEquals(streamRanges2, writer.getRangeRouter().getRanges());
        assertEquals(4, writers1.size());
        assertEquals(8, writers2.size());
        assertEquals(8, writer.getActiveWriters().size());
        assertEquals(8, writer.getRangeRouter().getRanges().getRanges().size());
        assertTrue(Maps.difference(writer.getActiveWriters(), writers2).areEqual());

        for (StreamRangeEventWriter<Integer, String> w : writers1.values()) {
            verify(w, times(1)).closeAndGetPendingWrites();
        }
        log.info("Verified closeAndGetPendingWrites() is called on first 4 writers");
    }

    private List<CompletableFuture<Position>> writeNumEvents(WriterImpl<Integer, String> writer,
                                                             int numEvents) {
        List<CompletableFuture<Position>> futures = Lists.newArrayList();
        for (int i = 0; i < numEvents; i++) {
            log.info("Write event {}.", i);
            CompletableFuture<Position> eventFuture = FutureUtils.createFuture();
            futures.add(eventFuture);
            WriteEvent<Integer, String> writeEvent = writeEventBuilder
                .withKey(i)
                .withValue("event-" + i)
                .withTimestamp(i)
                .build();
            writer.write(writeEvent, eventFuture);
        }
        return futures;
    }

    //
    // Write Batch Methods
    //

    @Test(expected = UnsupportedOperationException.class)
    public void testNewWriteBatch() throws Exception {
        when(mockMetaRangeClient.getActiveDataRanges())
            .thenReturn(FutureUtils.value(streamRanges1));
        WriterImpl<Integer, String> writer = createWriter();
        FutureUtils.result(writer.initialize());
        writer.newBatch();
    }

}
