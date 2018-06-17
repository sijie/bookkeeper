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

package org.apache.bookkeeper.clients.impl.stream;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.ReadEvent;
import org.apache.bookkeeper.api.stream.ReadEvents;
import org.apache.bookkeeper.api.stream.Reader;
import org.apache.bookkeeper.api.stream.ReaderConfig;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.clients.impl.internal.api.HashStreamRanges;
import org.apache.bookkeeper.clients.impl.internal.api.MetaRangeClient;
import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.impl.stream.ReaderImpl.State;
import org.apache.bookkeeper.clients.impl.stream.event.EventPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet;
import org.apache.bookkeeper.clients.impl.stream.event.RangePositionImpl;
import org.apache.bookkeeper.clients.impl.stream.event.ReadEventsImpl;
import org.apache.bookkeeper.clients.impl.stream.event.ReadEventsImpl.Recycler;
import org.apache.bookkeeper.clients.impl.stream.event.StreamPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeEventReader;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeEventReaderFactory;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.coder.VarIntCoder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.router.IntHashRouter;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.CompressionCodecType;
import org.apache.bookkeeper.stream.proto.EventSetType;
import org.apache.bookkeeper.stream.proto.RangeKeyType;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.readgroup.ReadGroupCommand;
import org.apache.bookkeeper.stream.proto.readgroup.ReadGroupCommand.Op;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.apache.bookkeeper.stream.protocol.util.ProtoUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test {@link ReaderImpl}.
 */
@Slf4j
public class ReaderImplTest {

    private static final long streamId = 12345L;

    @Rule
    public TestName runtime = new TestName();

    private final StreamProperties streamProps = StreamProperties.newBuilder()
        .setStorageContainerId(12345L)
        .setStreamConf(DEFAULT_STREAM_CONF)
        .setStreamId(streamId)
        .setStreamName("test-stream")
        .build();
    private final RootRangeClient mockRootRangeClient = mock(RootRangeClient.class);
    private final MetaRangeClient mockMetaRangeClient = mock(MetaRangeClient.class);
    private final StorageServerClientManager mockClientManager = mock(StorageServerClientManager.class);

    private final StreamConfig<Integer, String> streamConfig = StreamConfig.<Integer, String>builder()
        .keyCoder(VarIntCoder.of())
        .valueCoder(StringUtf8Coder.of())
        .keyRouter(IntHashRouter.of())
        .build();
    private final Map<String, Position> readPositions = Maps.newHashMap();
    private final LinkedBlockingQueue<ReadGroupCommand> commandQueue = new LinkedBlockingQueue<>();
    private LocalReadGroupController readGroup;

    private OrderedScheduler scheduler;

    private static String getEventData(long streamId, long rangeId, long eventId) {
        return "stream-" + streamId + "-range-" + rangeId + "-event-" + eventId;
    }

    private static ByteBuf getOneEventSetBuffer(long streamId,
                                                long rangeId,
                                                long eventId) {
        String data = getEventData(streamId, rangeId, eventId);
        EventSet.Writer<Integer, String> eventSetWriter = EventSet.<Integer, String>newWriterBuilder()
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(StringUtf8Coder.of())
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(1024)
            .withMaxNumEvents(8)
            .withWriterId(0)
            .withCompressionCodec(CompressionCodecType.NONE)
            .build();

        assertTrue(eventSetWriter.writeEvent(
            (int) rangeId, data, eventId));
        return eventSetWriter.complete();
    }

    @Before
    public void setup() {
        // positions
        int numRanges = 24;
        int numContainers = 8;
        List<RangeProperties> rangeList = ProtoUtils.split(
            streamId,
            numRanges,
            0L,
            (streamId, rangeId) -> ThreadLocalRandom.current().nextInt(numContainers));
        StreamPositionImpl streamPos = new StreamPositionImpl(streamProps.getStreamId());
        NavigableMap<Long, RangeProperties> rangeMap = new TreeMap<>();
        for (RangeProperties props : rangeList) {
            streamPos.addPosition(EventPositionImpl.createInitPos(props.getRangeId()));
            rangeMap.put(props.getStartHashKey(), props);
        }
        readPositions.put(streamProps.getStreamName(), streamPos);
        HashStreamRanges activeRanges = HashStreamRanges.ofHash(
            RangeKeyType.HASH,
            rangeMap
        );

        // mock clients
        when(mockRootRangeClient.getStream(eq(streamProps.getStreamId())))
            .thenReturn(FutureUtils.value(streamProps));
        when(mockMetaRangeClient.getActiveDataRanges()).thenReturn(FutureUtils.value(activeRanges));
        when(mockClientManager.openMetaRangeClient(any(StreamProperties.class)))
            .thenReturn(mockMetaRangeClient);
        when(mockClientManager.getRootRangeClient()).thenReturn(mockRootRangeClient);

        // scheduler
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .numThreads(1)
            .name("test-scheduler")
            .build();

        // create read group
        readGroup = spy(new LocalReadGroupController(
            runtime.getMethodName(),
            readPositions,
            mockClientManager,
            scheduler.chooseThread()));
        doAnswer(invocationOnMock -> {
            ReadGroupCommand command = invocationOnMock.getArgument(0);
            commandQueue.put(command);
            return invocationOnMock.callRealMethod();
        }).when(readGroup).onNext(any(ReadGroupCommand.class));
    }

    private ReaderImpl<Integer, String> createReader(StreamRangeEventReaderFactory factory,
                                                     ReaderConfig readerConfig)
            throws Exception {
        ReaderImpl<Integer, String> reader = new ReaderImpl<>(
            runtime.getMethodName(),
            streamConfig,
            readerConfig,
            mockClientManager,
            scheduler,
            readGroup,
            factory
        );
        FutureUtils.result(readGroup.start(reader));
        return reader;
    }

    private ReaderImpl<Integer, String> createReader(LocalReadGroupController readGroup) throws Exception {
        ReaderImpl<Integer, String> reader = new ReaderImpl<>(
            runtime.getMethodName(),
            streamConfig,
            ReaderConfig.builder().build(),
            mockClientManager,
            scheduler,
            readGroup
        );
        FutureUtils.result(readGroup.start(reader));
        return reader;
    }

    @Test
    public void testStartStop() throws Exception {
        // create a reader
        ReaderImpl<Integer, String> reader = createReader(readGroup);
        verify(readGroup, times(1)).start(same(reader));

        // start the reader
        Reader<Integer, String> startedReader = FutureUtils.result(reader.start());
        assertSame(reader, startedReader);
        assertEquals(State.ADDED, reader.getState());
        ReadGroupCommand command = commandQueue.take();
        assertEquals(Op.ADD_READER, command.getOp());
        verify(readGroup, times(1)).onNext(same(command));

        // stop the reader
        Reader<Integer, String> stoppedReader = FutureUtils.result(reader.stop());
        assertSame(reader, stoppedReader);
        assertEquals(State.REMOVED, reader.getState());
        command = commandQueue.take();
        assertEquals(Op.REM_READER, command.getOp());
        verify(readGroup, times(1)).onNext(same(command));

        // close the reader
        reader.close();
        assertEquals(State.CLOSE, reader.getState());
    }

    @Test
    public void testCloseShouldStopReader() throws Exception {
        // create a reader
        ReaderImpl<Integer, String> reader = createReader(readGroup);
        verify(readGroup, times(1)).start(same(reader));

        // start the reader
        Reader<Integer, String> startedReader = FutureUtils.result(reader.start());
        assertSame(reader, startedReader);
        assertEquals(State.ADDED, reader.getState());
        ReadGroupCommand command = commandQueue.take();
        assertEquals(Op.ADD_READER, command.getOp());
        verify(readGroup, times(1)).onNext(same(command));

        // close the reader
        reader.close();
        assertEquals(State.CLOSE, reader.getState());
        command = commandQueue.take();
        assertEquals(Op.REM_READER, command.getOp());
        verify(readGroup, times(1)).onNext(same(command));
    }

    @Test
    public void testCloseNonStartedReader() throws Exception {
        // create a reader
        ReaderImpl<Integer, String> reader = createReader(readGroup);
        verify(readGroup, times(1)).start(same(reader));

        // close the reader
        reader.close();
        assertEquals(State.CLOSE, reader.getState());
        // no command should be issued to the controller.
        assertNull(commandQueue.poll());
    }

    @Test
    public void testStopNonStartedReader() throws Exception {
        // create a reader
        ReaderImpl<Integer, String> reader = createReader(readGroup);
        verify(readGroup, times(1)).start(same(reader));

        // stop the reader
        try {
            FutureUtils.result(reader.stop());
            fail("Should fail to stop a non-started reader");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    @SuppressWarnings("unchecked")
    private StreamRangeEventReaderFactory createReaderFactory(ConcurrentMap<Long, StreamRangeEventReader> readers) {
        StreamRangeEventReaderFactory readerFactory = new StreamRangeEventReaderFactory() {
            @Override
            public <KeyT, ValueT> StreamRangeEventReader<KeyT, ValueT> createRangeEventReader(
                StreamConfig<KeyT, ValueT> streamConfig,
                StreamProperties streamProps,
                RangeProperties rangeProps,
                RangePosition rangePos,
                Recycler<KeyT, ValueT> readEventsRecycler) {

                StreamRangeEventReader<KeyT, ValueT> reader = readers.get(rangeProps.getRangeId());
                if (null != reader) {
                    return reader;
                }

                reader = mock(StreamRangeEventReader.class);

                final RangeId rid = RangeId.of(streamProps.getStreamId(), rangeProps.getRangeId());
                final AtomicLong sequencer = new AtomicLong(0L);
                when(reader.getRangeId()).thenReturn(rid);
                when(reader.initialize()).thenReturn(FutureUtils.value(reader));
                doAnswer(invocationOnMock -> {
                    log.info("Close range reader '{}'", rid);
                    return FutureUtils.Void();
                }).when(reader).closeAsync();
                doAnswer(invocationOnMock -> {
                    long sequence = sequencer.getAndIncrement();

                    ByteBuf eventBuf = getOneEventSetBuffer(
                        streamId,
                        rangeProps.getRangeId(),
                        sequence
                    );

                    EventSet.Reader<KeyT, ValueT> esReader = EventSet.<KeyT, ValueT>newReaderBuilder()
                        .withKeyCoder(streamConfig.keyCoder())
                        .withValueCoder(streamConfig.valueCoder())
                        .build(eventBuf);

                    ReadEventsImpl<KeyT, ValueT> readEvents = readEventsRecycler.create(
                        "test-stream",
                        rid,
                        esReader,
                        RangePositionImpl.create(
                            rangeProps.getRangeId(),
                            sequence,
                            sequence
                        ),
                        eventBuf.readableBytes()
                    );
                    return FutureUtils.value(readEvents);
                }).when(reader).readNext();

                readers.put(rangeProps.getRangeId(), reader);
                return reader;
            }
        };
        return readerFactory;
    }

    private Map<Long, Long> drainAndVerifyEvents(ReaderImpl<Integer, String> reader,
                                                 int numEvents,
                                                 long sequence)
            throws Exception {
        Map<Long, Long> receivedEvents = new HashMap<>();
        ReadEvents<Integer, String> readEvents;
        for (int i = 0; i < numEvents; i++) {
            readEvents = reader.readNext(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            assertEquals(1, readEvents.numEvents());
            ReadEvent<Integer, String> event = readEvents.next();
            assertNotNull(event);
            log.info("Received event : key = {}, value = {}, timestamp = {}, pos = {}",
                event.key(), event.value(), event.timestamp(), event.position());
            // verify the event
            long rangeId = event.key();
            String eventData = getEventData(streamId, rangeId, sequence);
            assertEquals(eventData, event.value());
            assertEquals(sequence, event.timestamp());
            // verify the position
            Position pos = event.position();
            assertTrue(pos instanceof EventPositionImpl);
            EventPositionImpl eventPos = (EventPositionImpl) pos;
            assertEquals(rangeId, eventPos.getRangeId());
            assertEquals(sequence, eventPos.getRangeOffset());
            assertEquals(sequence, eventPos.getRangeSeqNum());

            event.close();
            assertNull(readEvents.next());

            // record the events in `receivedEvents`
            receivedEvents.put(rangeId, event.timestamp());

            // release read events
            readEvents.close();
        }
        return receivedEvents;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBasicFlow() throws Exception {
        ConcurrentMap<Long, StreamRangeEventReader> readers = Maps.newConcurrentMap();
        StreamRangeEventReaderFactory readerFactory = createReaderFactory(readers);
        ReaderConfig readerConfig = ReaderConfig.builder()
            .maxReadAheadCacheSize(1)
            .build();
        ReaderImpl<Integer, String> reader = createReader(readerFactory, readerConfig);

        // start the reader
        Reader<Integer, String> startedReader = FutureUtils.result(reader.start());
        assertSame(reader, startedReader);

        int numRanges = DEFAULT_STREAM_CONF.getInitialNumRanges();

        // all ranges should be acquired and all readers should eventually go to pending queue
        while (reader.getNumPendingReaders() < numRanges) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // all readers go to pending state since cache is full
        assertEquals(numRanges, reader.getNumPendingReaders());
        // eventSetQueue should be full as well
        assertEquals(numRanges, reader.getEventSetQueue().size());
        // readers are all open
        assertEquals(numRanges, reader.getReaders().size());
        for (Map.Entry<RangeId, StreamRangeEventReader<Integer, String>> readerEntry : reader.getReaders().entrySet()) {
            RangeId rid = readerEntry.getKey();
            assertEquals(streamId, rid.getStreamId());
            long rangeId = rid.getRangeId();
            StreamRangeEventReader<Integer, String> expectedReader = readers.get(rangeId);
            assertSame(expectedReader, readerEntry.getValue());
        }

        // drain the readahead read events
        int numIterations = 10;
        for (int i = 0; i < numIterations; i++) {
            Map<Long, Long> receivedEvents = drainAndVerifyEvents(reader, numRanges, (long) i);
            assertEquals(numRanges, receivedEvents.size());
        }

        // stop the reader
        Reader<Integer, String> stoppedReader = FutureUtils.result(reader.stop());
        assertSame(reader, stoppedReader);
        assertEquals(State.REMOVED, reader.getState());

        // reader is stopped, means it is unregistered from local group controller
        // no more ranges will be assigned to reader
        assertEquals(numRanges, reader.getReaders().size());
        for (StreamRangeEventReader<Integer, String> sr : reader.getReaders().values()) {
            verify(sr, times(0)).closeAsync();
        }

        // close the reader will eventually close the open stream range readers.
        reader.close();
        assertEquals(State.CLOSE, reader.getState());
        assertEquals(numRanges, reader.getReaders().size());
        for (StreamRangeEventReader<Integer, String> sr : reader.getReaders().values()) {
            verify(sr, atLeast(1)).closeAsync();
        }
    }


}
