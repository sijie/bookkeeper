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

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.impl.stream.event.PendingEventSet;
import org.apache.bookkeeper.clients.impl.stream.event.PendingWrite;
import org.apache.bookkeeper.clients.impl.stream.event.RangePositionImpl;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.stream.proto.storage.SetupWriterResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetResponse;
import org.apache.bookkeeper.stream.proto.storage.WriteRequest;
import org.apache.bookkeeper.stream.proto.storage.WriteResponse;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.junit.Test;

/**
 * Unit test for {@link StreamRangeEventSetWriter}.
 */
public class StreamRangeEventSetWriterImplTest extends GrpcClientTestBase {

    private final long scId = 123L;
    private static final long rangeId = System.currentTimeMillis();
    private static final long writerId = System.currentTimeMillis();
    private static final String writerName = "test-writer-name";
    private static final RangeId range = RangeId.of(1234L, 5678L);
    private static final WriteResponse setupResponse = WriteResponse.newBuilder()
        .setCode(StatusCode.SUCCESS)
        .setSetupResp(SetupWriterResponse.newBuilder()
            .setWriterId(writerId)
            .setLastEventSetId(-1L))
        .build();
    private StorageContainerChannel scChannel;
    private ScheduledExecutorService executor;

    @Override
    public void doSetup() {
        scChannel = serverManager.getStorageContainerChannelManager().getOrCreate(scId);
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void doTeardown() {
        if (null != executor) {
            executor.shutdown();
        }
    }

    private static PendingWrite createPendingWrite(long sequenceId,
                                                   ByteBuf data,
                                                   CompletableFuture<RangePositionImpl> writeFuture) {
        return PendingWrite.of(
            sequenceId,
            PendingEventSet.of(data, Lists.newArrayList()),
            writeFuture);
    }

    private void ensureLastTaskExecuted() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        executor.submit(() -> latch.countDown());
        latch.await();
    }

    private RangePositionImpl createRangePos(int idx) {
        return RangePositionImpl.create(
            rangeId,
            (idx + 1) * 100,
            (idx + 1) * 1000
        );
    }

    private List<PendingWrite> prepareNRequests(int n) {
        long startSequenceId = 12340L;
        List<PendingWrite> writes = Lists.newArrayListWithExpectedSize(n);
        for (int i = 0; i < n; i++) {
            byte[] data = ("request-" + i).getBytes(UTF_8);
            PendingWrite write = createPendingWrite(
                startSequenceId + i,
                Unpooled.wrappedBuffer(data),
                FutureUtils.createFuture());
            writes.add(write);
        }
        return writes;
    }

    private List<WriteResponse> prepareNResponses(int n, StatusCode[] codes) {
        long startSequenceId = 12340L;
        List<WriteResponse> responses = Lists.newArrayListWithExpectedSize(n);
        for (int i = 0; i < n; i++) {
            WriteEventSetResponse writeEventSetResponse = WriteEventSetResponse.newBuilder()
                .setRangeId(rangeId)
                .setRangeOffset((i + 1) * 100)
                .setRangeSeqNum((i + 1) * 1000)
                .setEventSetId(startSequenceId + i)
                .build();
            WriteResponse writeResponse = WriteResponse.newBuilder()
                .setCode(codes[i])
                .setWriteResp(writeEventSetResponse)
                .build();
            responses.add(writeResponse);
        }
        return responses;
    }

    @SuppressWarnings("unchecked")
    private static <T> StreamObserver<T> mockStreamObserver() {
        return mock(StreamObserver.class);
    }

    @Test
    public void testCloseTimeoutPendingWrites() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);
        int numRequests = 5;
        List<PendingWrite> writes = prepareNRequests(numRequests);
        for (PendingWrite write : writes) {
            writeClient.write(write);
        }
        verify(requestObserver, times(numRequests)).onNext(any());
        FutureUtils.result(writeClient.closeAsync());
        assertNull(writeClient.getError());
        verify(requestObserver, times(1)).onCompleted();
        for (PendingWrite write : writes) {
            assertTrue(write.isDone());
            assertNotNull(write.getCause());
            assertTrue(write.getCause() instanceof InternalStreamException);
            InternalStreamException ise = (InternalStreamException) write.getCause();
            assertEquals(StatusCode.WRITE_TIMEOUT, ise.getCode());
        }
        assertEquals(0, writeClient.getNumPendingWrites());
    }

    @Test
    public void testFailPendingWritesOnError() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);
        int numRequests = 5;
        List<PendingWrite> writes = prepareNRequests(numRequests);
        for (PendingWrite write : writes) {
            writeClient.write(write);
        }
        verify(requestObserver, times(numRequests)).onNext(any());
        Exception cause = new IOException("mock-exception");
        writeClient.onError(cause);
        ensureLastTaskExecuted();
        FutureUtils.result(writeClient.closeAsync());
        assertNotNull(writeClient.getError());
        assertEquals(cause, writeClient.getError());
        for (PendingWrite write : writes) {
            assertTrue(write.isDone());
            assertNotNull(write.getCause());
            assertSame("Expected cause = " + cause + ", but " + write.getCause() + " is found.",
                cause, write.getCause());
        }
        assertEquals(0, writeClient.getNumPendingWrites());
    }

    @Test
    public void testFailPendingWritesOnCompleted() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);
        int numRequests = 5;
        List<PendingWrite> writes = prepareNRequests(numRequests);
        for (PendingWrite write : writes) {
            writeClient.write(write);
        }
        verify(requestObserver, times(numRequests)).onNext(any());
        writeClient.onCompleted();
        ensureLastTaskExecuted();
        FutureUtils.result(writeClient.closeAsync());
        assertNull(writeClient.getError());
        for (PendingWrite write : writes) {
            assertTrue(write.isDone());
            assertTrue(write.getCause() instanceof InternalStreamException);
            InternalStreamException ise = (InternalStreamException) write.getCause();
            assertEquals(StatusCode.WRITE_TIMEOUT, ise.getCode());
        }
        assertEquals(0, writeClient.getNumPendingWrites());
    }

    @Test
    public void testAllPendingWritesSucceed() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);
        int numRequests = 5;
        List<PendingWrite> writes = prepareNRequests(numRequests);
        for (PendingWrite write : writes) {
            writeClient.write(write);
        }
        verify(requestObserver, times(numRequests)).onNext(any());
        assertEquals(numRequests, writeClient.getNumPendingWrites());

        // parepare responses
        List<WriteResponse> responses = prepareNResponses(
            numRequests,
            new StatusCode[]{
                StatusCode.SUCCESS,
                StatusCode.SUCCESS,
                StatusCode.SUCCESS,
                StatusCode.SUCCESS,
                StatusCode.SUCCESS
            }
        );

        // deliver the responses to the client.
        int i = 0;
        for (WriteResponse response : responses) {
            writeClient.onNext(response);
            ensureLastTaskExecuted();
            PendingWrite write = writes.get(i);
            assertTrue(write.isDone());
            assertEquals(write.getSequenceId(), response.getWriteResp().getEventSetId());
            RangePositionImpl expectedPos = createRangePos(i);
            assertEquals(expectedPos, write.getRangePosition());
            assertNull(write.getCause());
            assertTrue(write.getWriteFuture().isDone());
            assertEquals(expectedPos, write.getWriteFuture().get());
            assertNull(write.getCause());
            for (int j = i + 1; j < numRequests; j++) {
                write = writes.get(j);
                assertFalse(write.isDone());
            }
            ++i;
        }

        FutureUtils.result(writeClient.closeAsync());
        assertNull(writeClient.getError());
        assertEquals(0, writeClient.getNumPendingWrites());
    }

    @Test
    public void testOutOfOrderAcknowledgments() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);
        int numRequests = 5;
        List<PendingWrite> writes = prepareNRequests(numRequests);
        for (PendingWrite write : writes) {
            writeClient.write(write);
        }
        verify(requestObserver, times(numRequests)).onNext(any());
        assertEquals(numRequests, writeClient.getNumPendingWrites());

        // parepare responses
        List<WriteResponse> responses = prepareNResponses(
            numRequests,
            new StatusCode[]{
                StatusCode.SUCCESS,
                StatusCode.SUCCESS,
                StatusCode.SUCCESS,
                StatusCode.SUCCESS,
                StatusCode.SUCCESS
            }
        );

        // deliver the responses to the client.
        completeWritesInDescendingOrder(writeClient, numRequests, writes, responses);

        // complete the first write
        writeClient.onNext(responses.get(0));
        ensureLastTaskExecuted();

        for (int i = 0; i < numRequests; i++) {
            PendingWrite write = writes.get(i);
            assertTrue(write.isDone());
            assertEquals(write.getSequenceId(), responses.get(i).getWriteResp().getEventSetId());
            RangePositionImpl expectedPos = createRangePos(i);
            assertEquals(expectedPos, write.getRangePosition());
            assertNull(write.getCause());
            assertTrue(write.getWriteFuture().isDone());
            assertEquals(expectedPos, write.getWriteFuture().get());
            assertNull(write.getCause());
        }

        FutureUtils.result(writeClient.closeAsync());
        assertNull(writeClient.getError());
        assertEquals(0, writeClient.getNumPendingWrites());
    }

    @Test
    public void testOutOfOrderAcknowledgmentsFailed() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);
        int numRequests = 5;
        List<PendingWrite> writes = prepareNRequests(numRequests);
        for (PendingWrite write : writes) {
            writeClient.write(write);
        }
        verify(requestObserver, times(numRequests)).onNext(any());
        assertEquals(numRequests, writeClient.getNumPendingWrites());

        // parepare responses
        List<WriteResponse> responses = prepareNResponses(
            numRequests,
            new StatusCode[]{
                StatusCode.FAILURE,
                StatusCode.SUCCESS,
                StatusCode.SUCCESS,
                StatusCode.SUCCESS,
                StatusCode.SUCCESS
            }
        );

        // deliver the responses to the client.
        completeWritesInDescendingOrder(writeClient, numRequests, writes, responses);

        // complete the first write
        writeClient.onNext(responses.get(0));
        ensureLastTaskExecuted();

        PendingWrite write = writes.get(0);
        assertTrue(write.isDone());
        assertNotNull(write.getCause());
        assertTrue(write.getCause() instanceof InternalStreamException);
        InternalStreamException writeException = (InternalStreamException) write.getCause();
        assertEquals(StatusCode.FAILURE, writeException.getCode());
        assertTrue(write.getWriteFuture().isDone());
        assertTrue(write.getWriteFuture().isCompletedExceptionally());
        assertTrue(writeClient.isClosed());

        for (int i = 1; i < numRequests; i++) {
            write = writes.get(i);
            assertTrue(write.isDone());
            assertEquals(write.getSequenceId(), responses.get(i).getWriteResp().getEventSetId());
            RangePositionImpl expectedPos = createRangePos(i);
            assertEquals(expectedPos, write.getRangePosition());
            assertNull(write.getCause());
            assertTrue(write.getWriteFuture().isDone());
            assertEquals(expectedPos, write.getWriteFuture().get());
            assertNull(write.getCause());
        }

        FutureUtils.result(writeClient.closeAsync());
        assertNull(writeClient.getError());
        assertEquals(0, writeClient.getNumPendingWrites());
    }

    private void completeWritesInDescendingOrder(StreamRangeEventSetWriterImpl writeClient,
                                                 int numRequests,
                                                 List<PendingWrite> writes,
                                                 List<WriteResponse> responses)
            throws InterruptedException {
        for (int i = numRequests - 1; i >= 1; i--) {
            WriteResponse response = responses.get(i);
            writeClient.onNext(response);
            ensureLastTaskExecuted();
            for (int j = 0; j < i; j++) {
                PendingWrite write = writes.get(j);
                assertFalse(write.isDone());
                assertNull(write.getRangePosition());
                assertNull(write.getCause());
            }
        }
    }

    @Test
    public void testConditionalWriteFailure() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);
        int numRequests = 5;
        List<PendingWrite> writes = prepareNRequests(numRequests);
        for (PendingWrite write : writes) {
            writeClient.write(write);
        }
        verify(requestObserver, times(numRequests)).onNext(any());
        assertEquals(numRequests, writeClient.getNumPendingWrites());

        int numResponses = 5;

        // parepare responses
        List<WriteResponse> responses = prepareNResponses(
            numResponses,
            new StatusCode[]{
                StatusCode.SUCCESS,
                StatusCode.SUCCESS,
                StatusCode.CONDITIONAL_WRITE_FAILURE,
                StatusCode.SUCCESS,
                StatusCode.SUCCESS
            }
        );

        // deliver the responses to the client.
        for (int i = 0; i < numResponses; i++) {
            WriteResponse response = responses.get(i);
            writeClient.onNext(response);
            ensureLastTaskExecuted();
            PendingWrite write = writes.get(i);
            assertTrue(write.isDone());
            if (i == 2) {
                assertNull(write.getRangePosition());
                assertNotNull(write.getCause());
                assertTrue(write.getCause() instanceof InternalStreamException);
                InternalStreamException writeException = (InternalStreamException) write.getCause();
                assertEquals(StatusCode.CONDITIONAL_WRITE_FAILURE, writeException.getCode());
            } else {
                assertEquals(write.getSequenceId(), response.getWriteResp().getEventSetId());
                RangePositionImpl expectedRangePos = createRangePos(i);
                assertEquals(expectedRangePos, write.getRangePosition());
                assertNull(write.getCause());
                assertTrue(write.getWriteFuture().isDone());
                assertEquals(expectedRangePos, write.getWriteFuture().get());
                assertNull(write.getCause());
            }
            for (int j = i + 1; j < numResponses; j++) {
                write = writes.get(j);
                assertFalse(write.isDone());
            }
        }

        assertEquals(0, writeClient.getNumPendingWrites());
        FutureUtils.result(writeClient.closeAsync());
        assertNull(writeClient.getError());
        assertEquals(0, writeClient.getNumPendingWrites());
    }

    @Test
    public void testStreamRangeFenced() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);
        int numRequests = 5;
        List<PendingWrite> writes = prepareNRequests(numRequests);
        for (PendingWrite write : writes) {
            writeClient.write(write);
        }
        verify(requestObserver, times(numRequests)).onNext(any());
        assertEquals(numRequests, writeClient.getNumPendingWrites());

        int numResponses = 3;

        // prepare responses
        List<WriteResponse> responses = prepareNResponses(
            numResponses,
            new StatusCode[]{
                StatusCode.SUCCESS,
                StatusCode.SUCCESS,
                StatusCode.STREAM_RANGE_FENCED,
            }
        );

        // deliver the responses to the client.
        for (int i = 0; i < numResponses - 1; i++) {
            WriteResponse response = responses.get(i);
            writeClient.onNext(response);
            ensureLastTaskExecuted();
            PendingWrite write = writes.get(i);
            assertTrue(write.isDone());
            assertEquals(write.getSequenceId(), response.getWriteResp().getEventSetId());
            RangePositionImpl rangePos = createRangePos(i);
            assertEquals(rangePos, write.getRangePosition());
            assertNull(write.getCause());
            assertTrue(write.getWriteFuture().isDone());
            assertEquals(rangePos, write.getWriteFuture().get());
            assertNull(write.getCause());
            for (int j = i + 1; j < numRequests; j++) {
                write = writes.get(j);
                assertFalse(write.isDone());
            }
            ++i;
        }

        // process the fenced response
        WriteResponse fencedResp = responses.get(numResponses - 1);
        writeClient.onNext(fencedResp);
        ensureLastTaskExecuted();
        assertTrue(writeClient.isFenced());
        assertTrue(writeClient.isClosed());

        for (int i = numResponses - 1; i < numRequests; i++) {
            PendingWrite write = writes.get(i);
            assertTrue(write.isDone());
            assertNull(write.getRangePosition());
            assertNotNull(write.getCause());
            assertTrue(write.getCause() instanceof InternalStreamException);
            InternalStreamException fencedException = (InternalStreamException) write.getCause();
            assertEquals(StatusCode.STREAM_RANGE_FENCED, fencedException.getCode());
        }

        assertEquals(0, writeClient.getNumPendingWrites());
        FutureUtils.result(writeClient.closeAsync());
        assertNull(writeClient.getError());
        assertEquals(0, writeClient.getNumPendingWrites());
    }

    @Test
    public void testWriteAfterClosed() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);
        FutureUtils.result(writeClient.closeAsync());
        try {
            writeClient.write(mock(PendingWrite.class));
            fail("Should fail write if the client is closed");
        } catch (InternalStreamException e) {
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof ObjectClosedException);
            ObjectClosedException oce = (ObjectClosedException) e.getCause();
            assertEquals(writeClient.toString() + " is already closed.", oce.getMessage());
        }
        verify(requestObserver, times(0)).onNext(any());
    }

    @Test
    public void testWriteAfterError() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);
        Exception testException = new IOException("test-write-after-error");
        writeClient.onError(testException);
        ensureLastTaskExecuted();
        assertTrue(testException == writeClient.getError());
        try {
            writeClient.write(mock(PendingWrite.class));
            fail("Should fail write if the client encounters error");
        } catch (InternalStreamException e) {
            assertNotNull(e.getCause());
            assertTrue(testException == e.getCause());
        }
        verify(requestObserver, times(0)).onNext(any());
        FutureUtils.result(writeClient.closeAsync());
    }

    @Test
    public void testWriteAfterFenced() throws Exception {
        StreamObserver<WriteRequest> requestObserver = mockStreamObserver();
        StreamRangeEventSetWriterImpl writeClient = new StreamRangeEventSetWriterImpl(
            range,
            writerName,
            executor,
            scChannel);
        writeClient.setRequestObserver(requestObserver);

        // prepare requests
        int numRequests = 1;
        List<PendingWrite> writes = prepareNRequests(numRequests);
        for (PendingWrite write : writes) {
            writeClient.write(write);
        }
        verify(requestObserver, times(numRequests)).onNext(any());
        assertEquals(numRequests, writeClient.getNumPendingWrites());

        // prepare responses
        List<WriteResponse> responses = prepareNResponses(
            numRequests,
            new StatusCode[]{
                StatusCode.STREAM_RANGE_FENCED,
            }
        );
        for (WriteResponse response : responses) {
            writeClient.onNext(response);
        }
        ensureLastTaskExecuted();
        try {
            writeClient.write(mock(PendingWrite.class));
            fail("Should fail write if the client encounters error");
        } catch (InternalStreamException e) {
            assertNull(e.getCause());
            assertEquals(StatusCode.STREAM_RANGE_FENCED, e.getCode());
        }
        verify(requestObserver, times(numRequests)).onNext(any());
        FutureUtils.result(writeClient.closeAsync());
    }

}
