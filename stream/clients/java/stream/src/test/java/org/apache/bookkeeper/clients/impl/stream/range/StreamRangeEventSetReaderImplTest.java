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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.storage.ReadEventSetResponse;
import org.apache.bookkeeper.stream.proto.storage.ReadRequest;
import org.apache.bookkeeper.stream.proto.storage.ReadResponse;
import org.apache.bookkeeper.stream.proto.storage.ReadServiceGrpc.ReadServiceImplBase;
import org.apache.bookkeeper.stream.proto.storage.SetupReaderRequest;
import org.apache.bookkeeper.stream.proto.storage.SetupReaderResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.junit.Test;

/**
 * Unit test for {@link StreamRangeEventSetReaderImpl}.
 */
@Slf4j
public class StreamRangeEventSetReaderImplTest extends GrpcClientTestBase {

    private final long scId = 123L;
    private final long maxCachedBytes = 1024 * 1024;
    private final RangeId range = RangeId.of(2344L, 5678L);
    private final RangePosition startPos = RangePosition.newBuilder()
        .setRangeId(range.getRangeId())
        .setOffset(1235L)
        .setSeqNum(2345L)
        .setSlotId(0)
        .build();
    private StorageContainerChannel scChannel;
    private StreamRangeEventSetReaderImpl reader;
    private CountDownLatch blockingLatch;

    @Override
    protected void doSetup() throws Exception {
        scChannel = serverManager.getStorageContainerChannelManager().getOrCreate(scId);
        reader = new StreamRangeEventSetReaderImpl(
            range,
            startPos,
            scheduler.chooseThread(range.getRangeId()),
            scChannel,
            maxCachedBytes,
            10);
    }

    @Override
    protected void doTeardown() throws Exception {
        if (null != blockingLatch) {
            blockingLatch.countDown();
        }
        if (null != reader) {
            reader.close();
        }
    }

    private void bindNopReadService() {
        ReadServiceImplBase readService = new ReadServiceImplBase() {
            @Override
            public StreamObserver<ReadRequest> read(StreamObserver<ReadResponse> responseObserver) {
                return new StreamObserver<ReadRequest>() {
                    @Override
                    public void onNext(ReadRequest value) {
                        // no-op
                    }

                    @Override
                    public void onError(Throwable t) {
                        // no-op
                    }

                    @Override
                    public void onCompleted() {
                        // no-op
                    }
                };
            }
        };
        serviceRegistry.addService(readService.bindService());
    }

    /**
     * TestCase: the reader will keep reconnect until it succeeds.
     */
    @Test
    public void testReconnectUtilSucceed() throws Exception {
        scChannel.setStorageServerChannelFuture(FutureUtils.exception(new Exception("failures")));
        FutureUtils.result(reader.initialize());

        CompletableFuture<ReadRequest> reqFuture = FutureUtils.createFuture();
        ReadServiceImplBase readService = new ReadServiceImplBase() {
            @Override
            public StreamObserver<ReadRequest> read(StreamObserver<ReadResponse> responseObserver) {
                return new StreamObserver<ReadRequest>() {
                    @Override
                    public void onNext(ReadRequest request) {
                        reqFuture.complete(request);
                    }

                    @Override
                    public void onError(Throwable t) {
                        reqFuture.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        // do nothing
                    }
                };
            }
        };
        serviceRegistry.addService(readService.bindService());
        while (reader.getReadSessionId().get() < 10) {
            TimeUnit.MILLISECONDS.sleep(10);
        }

        // update the scchannel to allow a success connect
        scChannel.setStorageServerChannelFuture(FutureUtils.value(
            new StorageServerChannel(
                InProcessChannelBuilder.forName(serverName).directExecutor().build(),
                Optional.empty()
            )
        ));

        ReadRequest receivedRequest = FutureUtils.result(reqFuture);
        assertEquals(ReadRequest.ReqCase.SETUP_REQ, receivedRequest.getReqCase());
        SetupReaderRequest startRequest = receivedRequest.getSetupReq();
        assertEquals(range.getStreamId(), startRequest.getStreamId());
        assertEquals(range.getRangeId(), startRequest.getRangeId());
        assertEquals(startPos.getRangeId(), startRequest.getRangeId());
        assertEquals(startPos.getOffset(), startRequest.getRangeOffset());
        assertEquals(startPos.getSeqNum(), startRequest.getRangeSeqNum());
        assertTrue(startRequest.getReadSessionId() >= 10);
    }

    /**
     * TestCase: closing the reader when the reader retries to reconnect to data range.
     */
    @Test
    public void testCloseOnReconnectLoop() throws Exception {
        scChannel.setStorageServerChannelFuture(FutureUtils.exception(new Exception("failures")));
        FutureUtils.result(reader.initialize());
        while (reader.getReadSessionId().get() < 10) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        reader.close();
        long sessionId = reader.getReadSessionId().get();
        TimeUnit.MILLISECONDS.sleep(50);
        assertEquals(sessionId, reader.getReadSessionId().get());
        assertNull(reader.getReconnectTask());
    }

    /**
     * TestCase: closing the reader will also close the open request observer.
     */
    @Test
    public void testCloseRequestObserverOnClose() throws Exception {
        CompletableFuture<ReadRequest> reqFuture = FutureUtils.createFuture();
        CompletableFuture<Void> doneFuture = FutureUtils.createFuture();
        ReadServiceImplBase readService = new ReadServiceImplBase() {
            @Override
            public StreamObserver<ReadRequest> read(StreamObserver<ReadResponse> responseObserver) {
                return new StreamObserver<ReadRequest>() {
                    @Override
                    public void onNext(ReadRequest request) {
                        reqFuture.complete(request);
                    }

                    @Override
                    public void onError(Throwable t) {
                        reqFuture.completeExceptionally(t);
                        doneFuture.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        doneFuture.complete(null);
                    }
                };
            }
        };
        serviceRegistry.addService(readService.bindService());
        FutureUtils.result(reader.initialize());

        ReadRequest receivedRequest = FutureUtils.result(reqFuture);
        assertEquals(ReadRequest.ReqCase.SETUP_REQ, receivedRequest.getReqCase());
        SetupReaderRequest startRequest = receivedRequest.getSetupReq();
        assertEquals(range.getStreamId(), startRequest.getStreamId());
        assertEquals(range.getRangeId(), startRequest.getRangeId());
        assertEquals(startPos.getRangeId(), startRequest.getRangeId());
        assertEquals(startPos.getOffset(), startRequest.getRangeOffset());
        assertEquals(startPos.getSeqNum(), startRequest.getRangeSeqNum());
        assertTrue(startRequest.getReadSessionId() >= 1);

        reader.close();
        // the request observer should be closed at this point.
        FutureUtils.result(doneFuture);
    }

    /**
     * TestCase: read next after a reader is closed.
     */
    @Test
    public void testReadNextAfterClosed() throws Exception {
        bindNopReadService();
        FutureUtils.result(reader.initialize());
        reader.close();
        try {
            FutureUtils.result(reader.readNext());
            fail("Should fail on reading next if the reader is closed.");
        } catch (ObjectClosedException oce) {
            // expected.
        }
    }

    /**
     * TestCase: all the pending read requests are cancelled when the reader is closed.
     */
    @Test
    public void testCloseCancellingAllPendingReads() throws Exception {
        bindNopReadService();
        FutureUtils.result(reader.initialize());
        int numRequests = 3;
        List<CompletableFuture<ReadData>> futures = Lists.newArrayListWithExpectedSize(numRequests);
        for (int i = 0; i < numRequests; i++) {
            futures.add(reader.readNext());
        }
        reader.close();
        for (CompletableFuture<ReadData> readFuture : futures) {
            try {
                FutureUtils.result(readFuture);
                fail("All the read requests should be failed with ObjectClosedException.");
            } catch (ObjectClosedException oce) {
                // expected
            }
        }
    }

    private LinkedList<ReadResponse> prepareEntries(int numEntries,
                                                    int entrySize,
                                                    boolean includeEndOfStream) {
        LinkedList<ReadResponse> dataQueue = Lists.newLinkedList();
        for (int i = 1; i <= numEntries; i++) {
            byte[] data = new byte[entrySize];
            ThreadLocalRandom.current().nextBytes(data);
            dataQueue.add(ReadResponse.newBuilder()
                .setCode(includeEndOfStream && i == numEntries ? StatusCode.END_OF_STREAM_RANGE : StatusCode.SUCCESS)
                .setReadSessionId(0)
                .setReadResp(ReadEventSetResponse.newBuilder()
                    .setData(ByteString.copyFrom(data))
                    .setRangeId(startPos.getRangeId())
                    .setRangeOffset(startPos.getOffset() + i)
                    .setRangeSeqNum(startPos.getSeqNum() + i))
                .build());
        }
        return dataQueue;
    }

    /**
     * TestCase: Test Background read.
     */
    @Test
    public void testBackgroundRead() throws Exception {
        LinkedList<ReadResponse> dataQueue = prepareEntries(3, 1024, false);
        List<ReadResponse> copiedDataQueue = Lists.newArrayList(dataQueue);

        final AtomicLong requestedSize = new AtomicLong(0L);
        final AtomicInteger sessionId = new AtomicInteger(-1);
        final AtomicReference<StreamObserver<ReadResponse>> respObserverRef = new AtomicReference<>(null);
        ReadServiceImplBase readService = new ReadServiceImplBase() {
            @Override
            public StreamObserver<ReadRequest> read(StreamObserver<ReadResponse> responseObserver) {
                respObserverRef.set(responseObserver);
                return new StreamObserver<ReadRequest>() {
                    @Override
                    public void onNext(ReadRequest request) {
                        switch (request.getReqCase()) {
                            case SETUP_REQ:
                                sessionId.set(request.getSetupReq().getReadSessionId());
                                break;
                            case READ_REQ:
                                requestedSize.addAndGet(request.getReadReq().getMaxNumBytes());
                                break;
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
            }
        };
        serviceRegistry.addService(readService.bindService());

        // initialize the reader
        FutureUtils.result(reader.initialize());
        log.info("Reader for range {} is initialized.", range);

        // block the execution of background read
        blockingLatch = new CountDownLatch(1);
        scheduler.chooseThread(range.getRangeId()).submit(() -> {
            log.info("Blocking the execution of background read.");
            try {
                blockingLatch.await();
            } catch (InterruptedException e) {
                // no-op;
            }
            log.info("The execution of background read is unblocked.");
        });

        // issue several read requests.
        int numRequests = 3;
        log.info("Issued {} read requests.", numRequests);
        List<CompletableFuture<ReadData>> readFutures = Lists.newArrayListWithExpectedSize(numRequests);
        for (int i = 0; i < numRequests; i++) {
            readFutures.add(reader.readNext());
        }

        assertEquals(1, reader.getScheduleCount().get());
        // unblock the background read, schedule count should reduce back to zero
        blockingLatch.countDown();
        log.info("Release the blocking latch for background read.");
        while (reader.getScheduleCount().get() > 0) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals(0, reader.getScheduleCount().get());
        assertEquals(numRequests, reader.getRequestQueue().size());

        // wait until the connection is connected
        while (null == respObserverRef.get()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }

        // sent 3 responses
        scheduler.chooseThread(range.getRangeId()).submit(() -> {
            respObserverRef.get().onNext(ReadResponse.newBuilder()
                .setReadSessionId(sessionId.get())
                .setSetupResp(SetupReaderResponse.newBuilder()
                    .build())
                .build());
            for (int i = 0; i < numRequests; i++) {
                ReadResponse response = dataQueue.removeFirst();
                respObserverRef.get().onNext(ReadResponse.newBuilder(response)
                    .setReadSessionId(sessionId.get())
                    .build());
            }
        });

        List<ReadData> readResponses = FutureUtils.result(FutureUtils.collect(readFutures));
        assertEquals(copiedDataQueue.size(), readResponses.size());
        verifyReadResponses(copiedDataQueue, numRequests, readResponses);
        assertEquals(
            startPos.getOffset() + numRequests,
            reader.getNextOffset());
        assertEquals(
            startPos.getSeqNum() + numRequests,
            reader.getNextSeqNum());
        assertEquals(0L, reader.getCachedBytes().get());
        assertEquals(maxCachedBytes + numRequests * 1024, reader.getRequestedSize().get());
        assertEquals(numRequests * 1024, reader.getReceivedSize().get());
        assertEquals(0, reader.getRequestQueue().size());
        // on the server side, we see same requested size
        assertEquals(maxCachedBytes + numRequests * 1024, requestedSize.get());
    }

    private void verifyReadResponses(List<ReadResponse> expectedReadResponses,
                                     int numRequests,
                                     List<ReadData> readResponses) {
        for (int i = 0; i < numRequests; i++) {
            ReadData readData = readResponses.get(i);
            ReadResponse expectedResp = expectedReadResponses.get(i);
            assertEquals(
                expectedResp.getReadResp().getRangeId(),
                readData.getRangeId());
            assertEquals(
                expectedResp.getReadResp().getRangeOffset(),
                readData.getRangeOffset());
            assertEquals(
                expectedResp.getReadResp().getRangeSeqNum(),
                readData.getRangeSeqNum());
            assertEquals(expectedResp.getReadResp().getData(), ByteString.copyFrom(readData.getData().nioBuffer()));
        }
    }

    /**
     * TestCase: Reader hit end of stream.
     */
    @Test
    public void testEndOfStream() throws Exception {
        LinkedList<ReadResponse> dataQueue = prepareEntries(3, 1024, true);

        final AtomicInteger readSession = new AtomicInteger(-1);
        ReadServiceImplBase readService = new ReadServiceImplBase() {
            @Override
            public StreamObserver<ReadRequest> read(StreamObserver<ReadResponse> responseObserver) {
                return new StreamObserver<ReadRequest>() {
                    @Override
                    public void onNext(ReadRequest request) {
                        if (request.hasSetupReq()) {
                            readSession.set(request.getSetupReq().getReadSessionId());
                            responseObserver.onNext(ReadResponse.newBuilder()
                                .setReadSessionId(readSession.get())
                                .setSetupResp(SetupReaderResponse.newBuilder()
                                    .build())
                                .build());
                        } else {
                            for (ReadResponse response : dataQueue) {
                                log.info("Send response {}", response);
                                responseObserver.onNext(ReadResponse.newBuilder(response)
                                    .setReadSessionId(readSession.get())
                                    .build());
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
            }
        };
        serviceRegistry.addService(readService.bindService());

        // initialize the reader
        FutureUtils.result(reader.initialize());
        log.info("Reader for range {} is initialized.", range);

        // issue several read requests.
        int numRequests = 5;
        log.info("Issued {} read requests.", numRequests);
        List<CompletableFuture<ReadData>> readFutures = Lists.newArrayListWithExpectedSize(numRequests);
        for (int i = 0; i < numRequests; i++) {
            readFutures.add(reader.readNext());
        }

        int idx = 0;
        for (CompletableFuture<ReadData> readFuture : readFutures) {
            if (idx < 2) {
                ReadData readData = FutureUtils.result(readFuture);
                ReadResponse readResp = dataQueue.get(idx);
                assertEquals(
                    readResp.getReadResp().getRangeId(),
                    readData.getRangeId());
                assertEquals(
                    readResp.getReadResp().getRangeOffset(),
                    readData.getRangeOffset());
                assertEquals(
                    readResp.getReadResp().getRangeSeqNum(),
                    readData.getRangeSeqNum());
                assertEquals(readResp.getReadResp().getData(), ByteString.copyFrom(readData.getData().nioBuffer()));
            } else {
                try {
                    FutureUtils.result(readFuture);
                    fail("Should hit end of stream");
                } catch (InternalStreamException ise) {
                    assertEquals(StatusCode.END_OF_STREAM_RANGE, ise.getCode());
                }
            }
            ++idx;
        }
    }

    /**
     * TestCase: Read Session is broken.
     */
    @Test
    public void testReadSessionBroken() throws Exception {
        AtomicReference<StreamObserver<ReadResponse>> respObserverRef = new AtomicReference<>(null);
        ReadServiceImplBase readService = new ReadServiceImplBase() {
            @Override
            public StreamObserver<ReadRequest> read(StreamObserver<ReadResponse> responseObserver) {
                respObserverRef.set(responseObserver);
                return new StreamObserver<ReadRequest>() {
                    @Override
                    public void onNext(ReadRequest request) {
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
            }
        };
        serviceRegistry.addService(readService.bindService());

        // initialize the reader
        FutureUtils.result(reader.initialize());
        log.info("Reader for range {} is initialized.", range);

        // issue several read requests.
        int numRequests = 5;
        log.info("Issued {} read requests.", numRequests);
        List<CompletableFuture<ReadData>> readFutures = Lists.newArrayListWithExpectedSize(numRequests);
        for (int i = 0; i < numRequests; i++) {
            readFutures.add(reader.readNext());
        }

        while (null == respObserverRef.get()) {
            TimeUnit.MILLISECONDS.sleep(20);
        }

        assertEquals(1, reader.getReadSessionId().get());
        scheduler.chooseThread(range.getRangeId()).submit(() -> {
            respObserverRef.get().onError(new StatusRuntimeException(Status.INTERNAL));
        });
        // the client will reconnect again
        while (reader.getReadSessionId().get() <= 1) {
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertEquals(2, reader.getReadSessionId().get());


        LinkedList<ReadResponse> dataQueue = prepareEntries(numRequests, 1024, false);

        // sent 2 responses with old session id
        scheduler.chooseThread(range.getRangeId()).submit(() -> {
            respObserverRef.get().onNext(ReadResponse.newBuilder()
                .setReadSessionId(1)
                .setSetupResp(SetupReaderResponse.newBuilder()
                    .build())
                .build());
            for (int i = 0; i < numRequests / 2; i++) {
                ReadResponse response = dataQueue.get(i);
                respObserverRef.get().onNext(ReadResponse.newBuilder(response)
                    .setReadSessionId(1)
                    .build());
            }
        });
        // sent 5 responses with new session id
        scheduler.chooseThread(range.getRangeId()).submit(() -> {
            respObserverRef.get().onNext(ReadResponse.newBuilder()
                .setReadSessionId(2)
                .setSetupResp(SetupReaderResponse.newBuilder()
                    .build())
                .build());
            for (int i = 0; i < numRequests; i++) {
                ReadResponse response = dataQueue.get(i);
                respObserverRef.get().onNext(ReadResponse.newBuilder(response)
                    .setReadSessionId(2)
                    .build());
            }
        });

        // the reader should not receive response from old session.
        List<ReadData> readResponses = FutureUtils.result(FutureUtils.collect(readFutures));
        assertEquals(dataQueue.size(), readResponses.size());
        verifyReadResponses(dataQueue, numRequests, readResponses);
    }

}
