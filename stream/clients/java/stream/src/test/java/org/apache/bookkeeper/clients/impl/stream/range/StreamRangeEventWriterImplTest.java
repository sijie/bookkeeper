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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.WriteEvent;
import org.apache.bookkeeper.api.stream.WriteEventBuilder;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.stream.event.EventPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet;
import org.apache.bookkeeper.clients.impl.stream.event.PendingWrite;
import org.apache.bookkeeper.clients.impl.stream.event.WriteEventBuilderImpl;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeEventWriterImpl.State;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.coder.VarIntCoder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.CompressionCodecType;
import org.apache.bookkeeper.stream.proto.EventSetType;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.storage.SetupWriterResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetRequest;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetResponse;
import org.apache.bookkeeper.stream.proto.storage.WriteRequest;
import org.apache.bookkeeper.stream.proto.storage.WriteResponse;
import org.apache.bookkeeper.stream.proto.storage.WriteServiceGrpc.WriteServiceImplBase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test Case for {@link StreamRangeEventWriterImpl}.
 */
@Slf4j
public class StreamRangeEventWriterImplTest extends GrpcClientTestBase {

    private abstract static class TestWriteRequestObserver implements StreamObserver<WriteRequest> {

        private StreamObserver<WriteResponse> respObserver = null;

        synchronized TestWriteRequestObserver setRespObserver(
            StreamObserver<WriteResponse> respObserver) {
            this.respObserver = respObserver;
            return this;
        }

        synchronized StreamObserver<WriteResponse> getRespObserver() {
            return this.respObserver;
        }

    }

    @Rule
    public final TestName name = new TestName();

    private static final long streamId = 1234L;
    private static final long rangeId = 3456L;
    private static final long groupId = 456L;
    private static final RangeProperties rangeMeta = RangeProperties.newBuilder()
        .setRangeId(rangeId)
        .setStorageContainerId(groupId)
        .setStartHashKey(Long.MAX_VALUE / 4)
        .setEndHashKey(Long.MAX_VALUE / 2)
        .build();
    private static final WriteResponse startWriterResp = WriteResponse.newBuilder()
        .setCode(StatusCode.SUCCESS)
        .setSetupResp(SetupWriterResponse.newBuilder()
            .setWriterId(0)
            .setLastEventSetId(-1L))
        .build();
    private final EventSet.WriterBuilder<Integer, String> eventSetWriterBuilder =
        EventSet.<Integer, String>newWriterBuilder()
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(8192)
            .withCompressionCodec(CompressionCodecType.NONE)
            .withMaxNumEvents(128)
            .withKeyCoder(VarIntCoder.of())
            .withValueCoder(StringUtf8Coder.of())
            .withWriterId(System.currentTimeMillis());
    private final WriteEventBuilder<Integer, String> writeEventBuilder = new WriteEventBuilderImpl<>();
    private StreamRangeClients rangeClients;
    private StreamRangeEventWriterImpl<Integer, String> rangeEventWriter;

    //
    // Write Service
    //

    private final AtomicReference<TestWriteRequestObserver> requestObserverProvider =
        new AtomicReference<>(null);
    private final WriteServiceImplBase writeService = new WriteServiceImplBase() {
        @Override
        public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> responseObserver) {
            requestObserverProvider.get().setRespObserver(responseObserver);
            return new StreamObserver<WriteRequest>() {
                @Override
                public void onNext(WriteRequest value) {
                    switch (value.getReqCase()) {
                        case SETUP_REQ:
                            responseObserver.onNext(startWriterResp);
                            break;
                        default:
                            requestObserverProvider.get().onNext(value);
                            break;
                    }
                }

                @Override
                public void onError(Throwable t) {
                    requestObserverProvider.get().onError(t);
                }

                @Override
                public void onCompleted() {
                    requestObserverProvider.get().onCompleted();
                }
            };
        }
    };

    @Override
    protected void doSetup() {
        // bind the write service
        serviceRegistry.addService(writeService.bindService());

        // build the client
        rangeClients = new StreamRangeClients(
            serverManager.getStorageContainerChannelManager(),
            scheduler
        );

        // set the channel
        CompletableFuture<StorageServerChannel> channelFuture = FutureUtils.createFuture();
        serverManager.getStorageContainerChannelManager()
            .getOrCreate(rangeMeta.getStorageContainerId())
            .setStorageServerChannelFuture(channelFuture);

        StorageServerChannel rsChannel = new StorageServerChannel(
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            Optional.empty());
        channelFuture.complete(rsChannel);
    }

    @Override
    protected void doTeardown() {
        if (null != rangeEventWriter) {
            rangeEventWriter.close();
        }
    }

    private StreamRangeEventWriterImpl<Integer, String> createDefaultStreamRangeEventWriterImpl() {
        return new StreamRangeEventWriterImpl<>(
            rangeClients,
            streamId,
            rangeMeta,
            "test-writer",
            eventSetWriterBuilder,
            Duration.ofMillis(10000),
            scheduler.chooseThread(rangeId));
    }

    private void ensureTaskExecuted() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        scheduler.chooseThread(rangeId).submit(() -> latch.countDown());
        latch.await();
    }

    @Test
    public void testBasicOpenClose() throws Exception {
        // set up the request observer
        TestWriteRequestObserver requestObserver = mock(TestWriteRequestObserver.class);
        requestObserverProvider.set(requestObserver);

        // create the eventset writer
        rangeEventWriter = createDefaultStreamRangeEventWriterImpl();

        // verify the created eventset writer
        assertEquals(0L, rangeEventWriter.getNumPendingBytes());
        assertEquals(0L, rangeEventWriter.getNumPendingEvents());
        assertEquals(0L, rangeEventWriter.getNumPendingEventSets());
        while (State.INITIALIZING == rangeEventWriter.getState()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals(State.INITIALIZED, rangeEventWriter.getState());

        // close the range writer
        rangeEventWriter.close();
        assertEquals(State.CLOSED, rangeEventWriter.getState());
        assertTrue(rangeEventWriter.getFlushTask().isCancelled());
    }

    @Test
    public void testCloseAndGetPendingWritesFlushEvents() throws Exception {
        // set up the request observer
        TestWriteRequestObserver requestObserver = mock(TestWriteRequestObserver.class);
        requestObserverProvider.set(requestObserver);

        // create the eventset writer
        EventSet.WriterBuilder<Integer, String> eventSetWriterBuilder =
            EventSet.<Integer, String>newWriterBuilder()
                .withEventSetType(EventSetType.DATA)
                .withBufferSize(81920)
                .withCompressionCodec(CompressionCodecType.NONE)
                .withMaxNumEvents(32)
                .withKeyCoder(VarIntCoder.of())
                .withValueCoder(StringUtf8Coder.of());
        if (null != rangeEventWriter) {
            rangeEventWriter.close();
        }
        rangeEventWriter = new StreamRangeEventWriterImpl<>(
            rangeClients,
            streamId,
            RangeProperties.newBuilder(rangeMeta)
                .setRangeId(rangeId + 1)
                .build(),
            "test-writer",
            eventSetWriterBuilder,
            Duration.ofMillis(-1L), // disable periodical flush
            scheduler.chooseThread(rangeId + 1));

        // write events
        int numEvents = 20;
        List<CompletableFuture<Position>> writeFutures = Lists.newArrayListWithExpectedSize(numEvents);
        for (int i = 0; i < numEvents; i++) {
            CompletableFuture<Position> writeFuture = FutureUtils.createFuture();
            writeFutures.add(writeFuture);
            WriteEvent<Integer, String> writeEvent = writeEventBuilder
                .withKey(i)
                .withValue("event-" + i)
                .withTimestamp(System.currentTimeMillis())
                .build();
            rangeEventWriter.write(
                writeEvent,
                writeFuture);
        }

        // verify the created eventset writer
        assertEquals(0L, rangeEventWriter.getNumPendingBytes());
        assertEquals(0L, rangeEventWriter.getNumPendingEvents());
        assertEquals(0L, rangeEventWriter.getNumPendingEventSets());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());

        // close the range writer
        List<PendingWrite> pendingWrites = rangeEventWriter.closeAndGetPendingWrites();
        assertEquals(State.CLOSED, rangeEventWriter.getState());
        assertEquals(
            "Expected only one pending write but " + pendingWrites.size() + " pending writes found : " + pendingWrites,
            1, pendingWrites.size());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());
    }

    @Test
    public void testCloseAndGetPendingWritesFromEmptyWriter() throws Exception {
        // set up the request observer
        TestWriteRequestObserver requestObserver = mock(TestWriteRequestObserver.class);
        requestObserverProvider.set(requestObserver);

        // create the eventset writer
        rangeEventWriter = createDefaultStreamRangeEventWriterImpl();

        // verify the created eventset writer
        assertEquals(0L, rangeEventWriter.getNumPendingBytes());
        assertEquals(0L, rangeEventWriter.getNumPendingEvents());
        assertEquals(0L, rangeEventWriter.getNumPendingEventSets());
        while (State.INITIALIZING == rangeEventWriter.getState()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals(State.INITIALIZED, rangeEventWriter.getState());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());

        // close the range writer
        List<PendingWrite> pendingWrites = rangeEventWriter.closeAndGetPendingWrites();
        assertEquals(State.CLOSED, rangeEventWriter.getState());
        assertTrue(rangeEventWriter.getFlushTask().isCancelled());
        assertEquals(0, pendingWrites.size());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());
    }

    @Test
    public void testCloseAndGetPendingWritesWhenNotInitialized() throws Exception {
        // set up the request observer
        TestWriteRequestObserver requestObserver = mock(TestWriteRequestObserver.class);
        requestObserverProvider.set(requestObserver);
        // clear the chanel future to make writes stuck
        CompletableFuture<StorageServerChannel> channelFuture = FutureUtils.createFuture();
        serverManager.getStorageContainerChannelManager()
            .getOrCreate(rangeMeta.getStorageContainerId())
            .setStorageServerChannelFuture(channelFuture);

        // create the eventset writer
        EventSet.WriterBuilder<Integer, String> eventSetWriterBuilder =
            EventSet.<Integer, String>newWriterBuilder()
                .withEventSetType(EventSetType.DATA)
                .withBufferSize(8192)
                .withCompressionCodec(CompressionCodecType.NONE)
                .withMaxNumEvents(8)
                .withKeyCoder(VarIntCoder.of())
                .withValueCoder(StringUtf8Coder.of());
        if (null != rangeEventWriter) {
            rangeEventWriter.close();
        }
        rangeEventWriter = new StreamRangeEventWriterImpl<>(
            rangeClients,
            streamId,
            rangeMeta,
            "test-writer",
            eventSetWriterBuilder,
            Duration.ofMillis(-1L), // disable periodical flush
            scheduler.chooseThread(rangeId));

        // write events
        int numEvents = 20;
        writeNumEvents(numEvents);

        // make sure all the events are attempted to transmit
        ensureTaskExecuted();

        // verify the pendings
        assertEquals(State.INITIALIZING, rangeEventWriter.getState());
        assertEquals(numEvents, rangeEventWriter.getNumPendingEvents());
        assertEquals(numEvents, rangeEventWriter.getNumPendingEventSets());
        assertTrue(rangeEventWriter.getNumPendingBytes() > 0);
        assertNotNull(rangeEventWriter.getPendingWrites());
        assertEquals(numEvents, rangeEventWriter.getPendingWrites().size());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());

        // close the range writer
        List<PendingWrite> pendingWrites = rangeEventWriter.closeAndGetPendingWrites();
        assertEquals(State.CLOSED, rangeEventWriter.getState());
        assertEquals(numEvents, pendingWrites.size());
        assertEquals(0, rangeEventWriter.getNumPendingEvents());
        assertEquals(0, rangeEventWriter.getNumPendingEventSets());
        assertEquals(0, rangeEventWriter.getNumPendingBytes());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());
    }

    @Test
    public void testCloseAndGetPendingWritesAfterInitialized() throws Exception {
        int numEvents = 20;
        CountDownLatch incomingRequestsLatch = new CountDownLatch(numEvents);
        List<WriteRequest> incomingRequests = Lists.newArrayListWithExpectedSize(numEvents);
        // set up the request observer
        TestWriteRequestObserver requestObserver = new TestWriteRequestObserver() {
            @Override
            public void onNext(WriteRequest request) {
                synchronized (incomingRequests) {
                    incomingRequests.add(request);
                }
                incomingRequestsLatch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                getRespObserver().onError(t);
            }

            @Override
            public void onCompleted() {
                getRespObserver().onCompleted();
            }
        };
        requestObserverProvider.set(requestObserver);
        // clear the chanel future to make setup-writer stuck
        CompletableFuture<StorageServerChannel> channelFuture = FutureUtils.createFuture();
        serverManager.getStorageContainerChannelManager()
            .getOrCreate(rangeMeta.getStorageContainerId())
            .setStorageServerChannelFuture(channelFuture);

        // create the eventset writer
        EventSet.WriterBuilder<Integer, String> eventSetWriterBuilder =
            EventSet.<Integer, String>newWriterBuilder()
                .withEventSetType(EventSetType.DATA)
                .withBufferSize(8192)
                .withCompressionCodec(CompressionCodecType.NONE)
                .withMaxNumEvents(8)
                .withKeyCoder(VarIntCoder.of())
                .withValueCoder(StringUtf8Coder.of());
        if (null != rangeEventWriter) {
            rangeEventWriter.close();
        }
        rangeEventWriter = new StreamRangeEventWriterImpl<>(
            rangeClients,
            streamId,
            rangeMeta,
            "test-writer",
            eventSetWriterBuilder,
            Duration.ofMillis(-1L), // disable periodical flush
            scheduler.chooseThread(rangeId));

        // write events
        writeNumEvents(numEvents);

        // make sure all the events are attempted to transmit
        ensureTaskExecuted();

        // verify the pending & unacked writes
        assertEquals(State.INITIALIZING, rangeEventWriter.getState());
        assertEquals(numEvents, rangeEventWriter.getNumPendingEvents());
        assertEquals(numEvents, rangeEventWriter.getNumPendingEventSets());
        assertTrue(rangeEventWriter.getNumPendingBytes() > 0);
        assertNotNull(rangeEventWriter.getPendingWrites());
        assertEquals(numEvents, rangeEventWriter.getPendingWrites().size());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());
        long numPendingBytes = rangeEventWriter.getNumPendingBytes();

        // completing setup-writer request
        StorageServerChannel rsChannel = new StorageServerChannel(
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            Optional.empty());
        channelFuture.complete(rsChannel);

        // make sure the writer is initialized
        while (State.INITIALIZING == rangeEventWriter.getState()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals(State.INITIALIZED, rangeEventWriter.getState());
        ensureTaskExecuted();

        // verify the requests are not pending but still unacked
        assertEquals(numEvents, rangeEventWriter.getNumPendingEvents());
        assertEquals(numEvents, rangeEventWriter.getNumPendingEventSets());
        assertEquals(numPendingBytes, rangeEventWriter.getNumPendingBytes());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(numEvents, rangeEventWriter.getUnackedWrites().size());

        incomingRequestsLatch.await();
        synchronized (incomingRequests) {
            assertEquals(numEvents, incomingRequests.size());
        }

        // close the range writer
        List<PendingWrite> pendingWrites = rangeEventWriter.closeAndGetPendingWrites();
        assertEquals(State.CLOSED, rangeEventWriter.getState());
        assertEquals(numEvents, pendingWrites.size());
        assertEquals(0, rangeEventWriter.getNumPendingEvents());
        assertEquals(0, rangeEventWriter.getNumPendingEventSets());
        assertEquals(0, rangeEventWriter.getNumPendingBytes());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());
    }

    private List<CompletableFuture<Position>> writeNumEvents(int numEvents) throws InternalStreamException {
        List<CompletableFuture<Position>> writeFutures = Lists.newArrayListWithExpectedSize(numEvents);
        for (int i = 0; i < numEvents; i++) {
            CompletableFuture<Position> writeFuture = FutureUtils.createFuture();
            writeFutures.add(writeFuture);
            WriteEvent<Integer, String> writeEvent = writeEventBuilder
                .withKey(i)
                .withValue("event-" + i)
                .withTimestamp(System.currentTimeMillis())
                .build();
            rangeEventWriter.write(
                writeEvent,
                writeFuture);
            rangeEventWriter.flush();
        }
        return writeFutures;
    }

    @Test
    public void testNormalWrites() throws Exception {
        int numEvents = 20;
        CountDownLatch incomingRequestsLatch = new CountDownLatch(numEvents);
        List<WriteRequest> incomingRequests = Lists.newArrayListWithExpectedSize(numEvents);
        // set up the request observer
        TestWriteRequestObserver requestObserver = new TestWriteRequestObserver() {
            @Override
            public void onNext(WriteRequest request) {
                log.info("Received write request {}.", request);
                synchronized (incomingRequests) {
                    incomingRequests.add(request);
                }
                incomingRequestsLatch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                getRespObserver().onError(t);
            }

            @Override
            public void onCompleted() {
                getRespObserver().onCompleted();
            }
        };
        requestObserverProvider.set(requestObserver);

        // create the eventset writer
        EventSet.WriterBuilder<Integer, String> eventSetWriterBuilder =
            EventSet.<Integer, String>newWriterBuilder()
                .withEventSetType(EventSetType.DATA)
                .withBufferSize(8192)
                .withCompressionCodec(CompressionCodecType.NONE)
                .withMaxNumEvents(8)
                .withKeyCoder(VarIntCoder.of())
                .withValueCoder(StringUtf8Coder.of());
        if (null != rangeEventWriter) {
            rangeEventWriter.close();
        }
        rangeEventWriter = new StreamRangeEventWriterImpl<>(
            rangeClients,
            streamId,
            rangeMeta,
            "test-writer",
            eventSetWriterBuilder,
            Duration.ofMillis(-1L), // disable periodical flush
            scheduler.chooseThread(rangeId));

        // write events
        List<CompletableFuture<Position>> writeFutures = writeNumEvents(numEvents);

        incomingRequestsLatch.await();
        synchronized (incomingRequests) {
            assertEquals(numEvents, incomingRequests.size());
        }

        // complete the writes
        long startOffset = 100L;
        long startSeqNum = 1000L;
        synchronized (incomingRequests) {
            long sequenceId = 0L;
            for (WriteRequest request : incomingRequests) {
                WriteEventSetRequest writeReq = request.getWriteReq();
                assertEquals(sequenceId, writeReq.getEventSetId());
                requestObserver
                    .getRespObserver()
                    .onNext(WriteResponse.newBuilder()
                        .setCode(StatusCode.SUCCESS)
                        .setWriteResp(WriteEventSetResponse.newBuilder()
                            .setEventSetId(writeReq.getEventSetId())
                            .setRangeId(rangeId)
                            .setRangeOffset(startOffset + sequenceId)
                            .setRangeSeqNum(startSeqNum + sequenceId))
                        .build());
                log.info("Completed write request {} - {}.", sequenceId, request);
                ++sequenceId;
            }
        }

        List<Position> results = FutureUtils.collect(writeFutures).get();

        assertEquals(numEvents, results.size());
        int idx = 0;
        for (Position pos : results) {
            assertTrue(pos instanceof EventPositionImpl);
            EventPositionImpl eventPos = (EventPositionImpl) pos;
            assertEquals(rangeId, eventPos.getRangeId());
            assertEquals(startOffset + idx, eventPos.getRangeOffset());
            assertEquals(startSeqNum + idx, eventPos.getRangeSeqNum());
            assertEquals(0, eventPos.getSlotId());
            ++idx;
        }
        assertEquals(State.INITIALIZED, rangeEventWriter.getState());
        assertEquals(0, rangeEventWriter.getNumPendingEvents());
        assertEquals(0, rangeEventWriter.getNumPendingEventSets());
        assertEquals(0, rangeEventWriter.getNumPendingBytes());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());

        // close the range writer
        List<PendingWrite> pendingWrites = rangeEventWriter.closeAndGetPendingWrites();
        assertEquals(State.CLOSED, rangeEventWriter.getState());
        assertEquals(0, pendingWrites.size());
        assertEquals(0, rangeEventWriter.getNumPendingEvents());
        assertEquals(0, rangeEventWriter.getNumPendingEventSets());
        assertEquals(0, rangeEventWriter.getNumPendingBytes());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());
    }

    @Test
    public void testPeriodicalFlush() throws Exception {
        int numEvents = 20;
        AtomicLong sequenceId = new AtomicLong(0L);
        long startOffset = 100L;
        long startSeqNum = 1000L;
        AtomicLong nextOffset = new AtomicLong(startOffset);
        AtomicLong nextSeqNum = new AtomicLong(startSeqNum);
        // set up the request observer
        TestWriteRequestObserver requestObserver = new TestWriteRequestObserver() {
            @Override
            public void onNext(WriteRequest request) {
                log.info("Received write request {}.", request);
                getRespObserver()
                    .onNext(WriteResponse.newBuilder()
                        .setCode(StatusCode.SUCCESS)
                        .setWriteResp(WriteEventSetResponse.newBuilder()
                            .setEventSetId(request.getWriteReq().getEventSetId())
                            .setRangeId(rangeId)
                            .setRangeOffset(nextOffset.getAndIncrement())
                            .setRangeSeqNum(nextSeqNum.getAndIncrement())
                        )
                        .build());
                log.info("Completed write request {} - {}.", sequenceId.getAndIncrement(), request);
            }

            @Override
            public void onError(Throwable t) {
                getRespObserver().onError(t);
            }

            @Override
            public void onCompleted() {
                getRespObserver().onCompleted();
            }
        };
        requestObserverProvider.set(requestObserver);

        // create the eventset writer
        EventSet.WriterBuilder<Integer, String> eventSetWriterBuilder =
            EventSet.<Integer, String>newWriterBuilder()
                .withEventSetType(EventSetType.DATA)
                .withBufferSize(8192)
                .withCompressionCodec(CompressionCodecType.NONE)
                .withMaxNumEvents(8)
                .withKeyCoder(VarIntCoder.of())
                .withValueCoder(StringUtf8Coder.of());
        if (null != rangeEventWriter) {
            rangeEventWriter.close();
        }
        rangeEventWriter = new StreamRangeEventWriterImpl<>(
            rangeClients,
            streamId,
            rangeMeta,
            "test-writer",
            eventSetWriterBuilder,
            Duration.ofMillis(5), // flush every 5 ms
            scheduler.chooseThread(rangeId));

        // write events
        List<CompletableFuture<Position>> writeFutures = Lists.newArrayListWithExpectedSize(numEvents);
        for (int i = 0; i < numEvents; i++) {
            CompletableFuture<Position> writeFuture = FutureUtils.createFuture();
            writeFutures.add(writeFuture);
            WriteEvent<Integer, String> writeEvent = writeEventBuilder
                .withKey(i)
                .withValue("event-" + i)
                .withTimestamp(System.currentTimeMillis())
                .build();
            rangeEventWriter.write(
                writeEvent,
                writeFuture);
            Position pos = writeFuture.get();
            assertTrue(pos instanceof EventPositionImpl);
            EventPositionImpl eventPos = (EventPositionImpl) pos;
            assertEquals(rangeId, eventPos.getRangeId());
            assertEquals(startOffset + i, eventPos.getRangeOffset());
            assertEquals(startSeqNum + i, eventPos.getRangeSeqNum());
            assertEquals(0, eventPos.getSlotId());
        }

        assertEquals(State.INITIALIZED, rangeEventWriter.getState());
        assertEquals(0, rangeEventWriter.getNumPendingEvents());
        assertEquals(0, rangeEventWriter.getNumPendingEventSets());
        assertEquals(0, rangeEventWriter.getNumPendingBytes());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());

        // close the range writer
        List<PendingWrite> pendingWrites = rangeEventWriter.closeAndGetPendingWrites();
        assertEquals(State.CLOSED, rangeEventWriter.getState());
        assertEquals(0, pendingWrites.size());
        assertEquals(0, rangeEventWriter.getNumPendingEvents());
        assertEquals(0, rangeEventWriter.getNumPendingEventSets());
        assertEquals(0, rangeEventWriter.getNumPendingBytes());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());
    }

    @Test
    public void testWriteFenced() throws Exception {
        int numEvents = 20;
        // set up the request observer
        TestWriteRequestObserver requestObserver = new TestWriteRequestObserver() {
            @Override
            public void onNext(WriteRequest request) {
                log.info("Received write request {}.", request);
                getRespObserver()
                    .onNext(WriteResponse.newBuilder()
                        .setCode(StatusCode.STREAM_RANGE_FENCED)
                        .setWriteResp(WriteEventSetResponse.newBuilder()
                            .setEventSetId(request.getWriteReq().getEventSetId()))
                        .build());
            }

            @Override
            public void onError(Throwable t) {
                getRespObserver().onError(t);
            }

            @Override
            public void onCompleted() {
                getRespObserver().onCompleted();
            }
        };
        requestObserverProvider.set(requestObserver);

        // create the eventset writer
        EventSet.WriterBuilder<Integer, String> eventSetWriterBuilder =
            EventSet.<Integer, String>newWriterBuilder()
                .withEventSetType(EventSetType.DATA)
                .withBufferSize(8192)
                .withCompressionCodec(CompressionCodecType.NONE)
                .withMaxNumEvents(8)
                .withKeyCoder(VarIntCoder.of())
                .withValueCoder(StringUtf8Coder.of());
        if (null != rangeEventWriter) {
            rangeEventWriter.close();
        }
        rangeEventWriter = new StreamRangeEventWriterImpl<>(
            rangeClients,
            streamId,
            rangeMeta,
            "test-writer",
            eventSetWriterBuilder,
            Duration.ofMillis(5), // flush every 5 ms
            scheduler.chooseThread(rangeId));

        // write events
        List<CompletableFuture<Position>> writeFutures = Lists.newArrayListWithExpectedSize(numEvents);
        for (int i = 0; i < numEvents; i++) {
            CompletableFuture<Position> writeFuture = FutureUtils.createFuture();
            writeFutures.add(writeFuture);
            try {
                WriteEvent<Integer, String> writeEvent = writeEventBuilder
                    .withKey(i)
                    .withValue("event-" + i)
                    .withTimestamp(System.currentTimeMillis())
                    .build();
                rangeEventWriter.write(
                    writeEvent,
                    writeFuture);
                if (i > 0) {
                    fail("Should fail if the data range is fenced on write request " + i);
                }
            } catch (InternalStreamException ise) {
                if (StatusCode.STREAM_RANGE_FENCED != ise.getCode()) {
                    throw ise;
                }
                if (i == 0) {
                    fail("Should not fail if the data range is fenced");
                }
            }
            if (i == 0) {
                try {
                    writeFuture.get();
                    fail("should fail since the data range is fenced");
                } catch (ExecutionException ee) {
                    assertNotNull(ee.getCause());
                    assertTrue(ee.getCause() instanceof InternalStreamException);
                    InternalStreamException ise = (InternalStreamException) (ee.getCause());
                    assertEquals(StatusCode.STREAM_RANGE_FENCED, ise.getCode());
                }
            }
        }

        assertEquals(State.FENCED, rangeEventWriter.getState());
        assertEquals(1, rangeEventWriter.getNumPendingEvents());
        assertEquals(1, rangeEventWriter.getNumPendingEventSets());
        assertTrue(rangeEventWriter.getNumPendingBytes() > 0);
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(1, rangeEventWriter.getUnackedWrites().size());

        // close the range writer
        List<PendingWrite> pendingWrites = rangeEventWriter.closeAndGetPendingWrites();
        assertEquals(State.FENCED, rangeEventWriter.getState());
        assertEquals(1, pendingWrites.size());
        assertEquals(0, rangeEventWriter.getNumPendingEvents());
        assertEquals(0, rangeEventWriter.getNumPendingEventSets());
        assertEquals(0, rangeEventWriter.getNumPendingBytes());
        assertNull(rangeEventWriter.getPendingWrites());
        assertEquals(0, rangeEventWriter.getUnackedWrites().size());
    }

}
