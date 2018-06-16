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

package org.apache.bookkeeper.clients.impl.stream.range;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.Unpooled;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.impl.stream.exceptions.EndOfStreamRangeException;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.storage.ReadEventSetRequest;
import org.apache.bookkeeper.stream.proto.storage.ReadEventSetResponse;
import org.apache.bookkeeper.stream.proto.storage.ReadRequest;
import org.apache.bookkeeper.stream.proto.storage.ReadResponse;
import org.apache.bookkeeper.stream.proto.storage.SetupReaderRequest;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * A default implementation of {@link StreamRangeEventSetReader}.
 */
@Slf4j
public class StreamRangeEventSetReaderImpl
    extends AbstractAutoAsyncCloseable
    implements StreamRangeEventSetReader, StreamObserver<ReadResponse>, Runnable {

    private final RangeId range;
    private final ScheduledExecutorService executor;
    private final StorageContainerChannel scChannel;
    private final long maxReadAheadBytes;
    private final long reconnectBackoffMs;

    //
    // Reader State
    //

    private final LinkedList<ReadResponse> entryQueue;
    private final AtomicLong cachedBytes = new AtomicLong(0L);
    private final AtomicLong requestedSize = new AtomicLong(0L);
    private final AtomicLong receivedSize = new AtomicLong(0L);
    private final AtomicLong scheduleCount = new AtomicLong(0L);
    private final AtomicInteger readSessionId = new AtomicInteger(0);
    private StreamObserver<ReadRequest> requestObserver;
    private long nextRangeOffset;
    private long nextRangeSeqNum;
    private final LinkedList<CompletableFuture<ReadData>> requestQueue;
    private Throwable cause = null;
    private ScheduledFuture<?> reconnectTask = null;
    private boolean connected = false;

    StreamRangeEventSetReaderImpl(RangeId range,
                                  RangePosition rangePos,
                                  ScheduledExecutorService executor,
                                  StorageContainerChannel channel,
                                  long maxReadAheadBytes,
                                  long reconnectBackoffMs) {
        this.range = range;
        this.executor = executor;
        this.scChannel = channel;
        this.maxReadAheadBytes = maxReadAheadBytes;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.entryQueue = Lists.newLinkedList();
        this.nextRangeOffset = rangePos.getOffset();
        this.nextRangeSeqNum = rangePos.getSeqNum();
        this.requestQueue = Lists.newLinkedList();
        if (log.isDebugEnabled()) {
            log.debug("Construct an event set reader on range {} starting at pos : offset = {}, seq num = {}",
                range, nextRangeOffset, nextRangeSeqNum);
        }
    }

    @VisibleForTesting
    AtomicInteger getReadSessionId() {
        return readSessionId;
    }

    @VisibleForTesting
    synchronized ScheduledFuture<?> getReconnectTask() {
        return reconnectTask;
    }

    @VisibleForTesting
    AtomicLong getCachedBytes() {
        return cachedBytes;
    }

    @VisibleForTesting
    AtomicLong getRequestedSize() {
        return requestedSize;
    }

    @VisibleForTesting
    AtomicLong getReceivedSize() {
        return receivedSize;
    }

    @VisibleForTesting
    AtomicLong getScheduleCount() {
        return scheduleCount;
    }

    @VisibleForTesting
    synchronized LinkedList<CompletableFuture<ReadData>> getRequestQueue() {
        return requestQueue;
    }

    @VisibleForTesting
    LinkedList<ReadResponse> getEntryQueue() {
        return entryQueue;
    }

    @VisibleForTesting
    long getNextOffset() {
        return nextRangeOffset;
    }

    @VisibleForTesting
    long getNextSeqNum() {
        return nextRangeSeqNum;
    }

    //
    // Methods for Response Handling
    //

    @Override
    public void onNext(ReadResponse readResponse) {
        if (log.isTraceEnabled()) {
            log.trace("Received read response {}", readResponse);
        }
        if (readResponse.getReadSessionId() < readSessionId.get()) {
            // ignore messages coming from old session
            if (log.isDebugEnabled()) {
                log.debug("Received events for range {} from older session {} : current session = {}",
                    range, readResponse.getReadSessionId(), readSessionId.get());
            }
            return;
        }

        synchronized (this) {
            if (connected) {
                // connected
                if (readResponse.hasSetupResp()) {
                    log.error("Received setup response on a channel that has completed setup on reading range"
                        + " {} from storage container {} at session {}",
                        range, scChannel.getStorageContainerId(), readResponse.getReadSessionId());
                    onError(new IllegalStateException(
                        "Received setup response on a channel that has completed setup"));
                    return;
                } else {
                    executor.submit(() -> processEventSetResponse(readResponse));
                }
            } else {
                // not connected yet
                if (readResponse.hasSetupResp()) {
                    log.info("Reader connected to range {} at storage container {} at session {}",
                        range, scChannel.getStorageContainerId(), readResponse.getReadSessionId());
                    connected = true;
                    executor.submit(() -> processSetupResponse(readResponse));
                } else {
                    log.error("Received read response on a channel before finishing setup on reading range"
                        + " {} from storage container {} at session {}",
                        range, scChannel.getStorageContainerId(), readResponse.getReadSessionId());
                    onError(new IllegalStateException("Received event set response before finishing setup"));
                    return;
                }
            }
        }
    }

    private void processSetupResponse(ReadResponse readResponse) {
        if (StatusCode.SUCCESS == readResponse.getCode()) {
            readMoreBytes(maxReadAheadBytes);
        } else {
            onError(new InternalStreamException(
                readResponse.getCode(),
                "Failed to setup reader on range " + range));
        }
    }

    private void processEventSetResponse(ReadResponse readResponse) {
        ReadEventSetResponse resp = readResponse.getReadResp();
        StatusCode code = readResponse.getCode();
        if (StatusCode.SUCCESS == code || StatusCode.END_OF_STREAM_RANGE == code) {
            receivedSize.addAndGet(resp.getData().size());
            long lastOffset = nextRangeOffset;
            long lastSeqNum = nextRangeSeqNum;
            long newOffset = resp.getRangeOffset();
            long newSeqNum = resp.getRangeSeqNum();
            if (newOffset < lastOffset || newSeqNum < lastSeqNum) {
                log.error("Received out-of-order events on reading range {} at session {}.",
                    range, readResponse.getReadSessionId());
                return;
            }
            entryQueue.add(readResponse);
            cachedBytes.addAndGet(resp.getData().size());
            nextRangeOffset = newOffset;
            nextRangeSeqNum = newSeqNum;
            scheduleBackgroundRead();
        } else {
            // server will terminate the stream when error occurs. #onError will be called.
        }
    }

    @Override
    public void onError(Throwable throwable) {
        reconnect(reconnectBackoffMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onCompleted() {
        // do nothing
    }

    private long getNextReadBytes() {
        if (cachedBytes.get() >= maxReadAheadBytes) {
            return 0L;
        }
        long numBytesToFillCache = maxReadAheadBytes - (int) cachedBytes.get();
        long numBytesToRead = maxReadAheadBytes - (requestedSize.get() - receivedSize.get());
        return Math.max(0L, Math.min(numBytesToFillCache, numBytesToRead));
    }

    private void readMoreBytes(long moreBytes) {
        if (moreBytes <= 0L) {
            return;
        }

        requestObserver.onNext(ReadRequest.newBuilder()
            .setReadReq(ReadEventSetRequest.newBuilder()
                .setMaxNumBytes(moreBytes))
            .build()
        );
        long newSize = requestedSize.addAndGet(moreBytes);
        if (log.isDebugEnabled()) {
            log.debug("read more bytes {}, total = {} at read session {}", moreBytes, newSize, readSessionId.get());
        }
    }

    private synchronized void reconnect(long delay, TimeUnit timeUnit) {
        if (null != reconnectTask) {
            return;
        }

        log.info("Reconnect to range {} @ storage container {} in {} {}.",
            range, scChannel.getStorageContainerId(), delay, timeUnit);

        // reset the state
        connected = false;
        // reset the counter
        requestedSize.set(0L);
        receivedSize.set(0L);

        reconnectTask = executor.schedule(() -> {
            synchronized (this) {
                reconnectTask = null;
            }
            CompletableFuture<Void> connectFuture = FutureUtils.createFuture();
            connect(connectFuture);
        }, delay, timeUnit);
    }

    private void connect(CompletableFuture<Void> connectFuture) {
        int sessionId = readSessionId.incrementAndGet();
        scChannel.getStorageContainerChannelFuture().whenCompleteAsync((rsChannel, cause) -> {
            if (null != cause) {
                scheduleReconnect(connectFuture);
                return;
            }
            log.info("Connected to range {} @ storage container {}",
                range, scChannel.getStorageContainerId());

            final long offset = nextRangeOffset;
            final long seqNum = nextRangeSeqNum;

            this.requestObserver =
                rsChannel.getStreamReadService().read(this);

            ReadRequest setupRequest = ReadRequest.newBuilder()
                .setSetupReq(SetupReaderRequest.newBuilder()
                    .setReadSessionId(sessionId)
                    .setStreamId(range.getStreamId())
                    .setRangeId(range.getRangeId())
                    .setRangeOffset(offset)
                    .setRangeSeqNum(seqNum))
                .build();
            this.requestObserver.onNext(setupRequest);
            if (log.isDebugEnabled()) {
                log.debug("Setup reader reading range {} @ storage container {} at session {} : {}",
                    range, scChannel.getStorageContainerId(), sessionId, setupRequest);
            }
        }, executor);
    }

    private void scheduleReconnect(CompletableFuture<Void> connectFuture) {
        if (isClosed()) {
            log.info("Reader for range {} is already closed, skipped reconnecting.", range);
            // if the reader is already closed, don't reconnect
            connectFuture.complete(null);
            return;
        }
        reconnect(reconnectBackoffMs, TimeUnit.MILLISECONDS);
    }

    //
    // Reader Methods
    //

    @Override
    public CompletableFuture<Void> initialize() {
        reconnect(0, TimeUnit.MILLISECONDS);
        return FutureUtils.value(null);
    }

    @Override
    public synchronized CompletableFuture<ReadData> readNext() {
        CompletableFuture<ReadData> future = FutureUtils.createFuture();
        if (null != cause) {
            failReadRequest(future, cause);
            return future;
        }
        if (isClosed()) {
            failReadRequest(future, new ObjectClosedException("StreamRangeEventSetReader(" + range + ")"));
            return future;
        }
        boolean scheduleRead = requestQueue.isEmpty();
        requestQueue.add(future);
        if (scheduleRead) {
            scheduleBackgroundRead();
        }
        return future;
    }

    private void failReadRequest(CompletableFuture<ReadData> future,
                                 Throwable t) {
        executor.submit(() -> future.completeExceptionally(t));
    }

    private synchronized void scheduleBackgroundRead() {
        if (isClosed()) {
            return;
        }

        long prevCount = scheduleCount.getAndIncrement();
        if (0 == prevCount) {
            executor.submit(this);
        }
    }

    private synchronized void setLastException(Throwable t) {
        if (null == cause) {
            cause = t;
        }
        cancelAllPendingReads(cause);
    }

    private synchronized void cancelAllPendingReads(Throwable cause) {
        if (!requestQueue.isEmpty()) {
            log.info("Cancelling all the pending reads on data range {}.", range);
        }
        for (CompletableFuture<ReadData> request : requestQueue) {
            request.completeExceptionally(cause);
        }
        if (!requestQueue.isEmpty()) {
            log.info("{} pending reads on data range {} are cancelled.", requestQueue.size(), range);
        }
        requestQueue.clear();
    }

    // schedule background read
    @Override
    public void run() {
        if (isClosed()) {
            return;
        }

        long scheduleCountLocal = scheduleCount.decrementAndGet();
        while (true) {
            CompletableFuture<ReadData> nextRead;
            boolean encounteredError;
            synchronized (this) {
                nextRead = requestQueue.peek();

                // queue is empty, nothing to read, return
                if (null == nextRead) {
                    scheduleCount.set(0L);
                    break;
                }

                encounteredError = null != cause;
            }

            if (!encounteredError) {
                if (nextRead.isDone()) {
                    // user cancel the request
                    setLastException(new InterruptedException("Interrupted on reading data range " + range));
                    return;
                }
            }

            ReadResponse readResp = entryQueue.poll();
            if (null != readResp) { // response is available

                CompletableFuture<ReadData> removedRead = requestQueue.poll();
                if (null == removedRead || removedRead != nextRead) {
                    IllegalStateException ise =
                        new IllegalStateException("Unexpected condition at reading response " + readResp);
                    nextRead.completeExceptionally(ise);
                    if (null != removedRead) {
                        removedRead.completeExceptionally(ise);
                    }
                    setLastException(ise);
                    return;
                }

                if (StatusCode.END_OF_STREAM_RANGE == readResp.getCode()) {
                    log.info("Reach end of stream range {}.", range);
                    EndOfStreamRangeException eos =
                        new EndOfStreamRangeException("End of Stream reached on data range " + range);
                    nextRead.completeExceptionally(eos);
                    setLastException(eos);
                    return;
                } else {
                    ReadEventSetResponse readEventSetResponse = readResp.getReadResp();
                    long rid = readEventSetResponse.getRangeId();
                    long offset = readEventSetResponse.getRangeOffset();
                    long seqNum = readEventSetResponse.getRangeSeqNum();
                    ByteString data = readEventSetResponse.getData();
                    cachedBytes.addAndGet(-data.size());
                    nextRead.complete(ReadDataImpl.create(
                        rid,
                        offset,
                        seqNum,
                        Unpooled.wrappedBuffer(data.asReadOnlyByteBuffer())));
                }

            } else {
                if (0L == scheduleCountLocal) {
                    break;
                }
                scheduleCountLocal = scheduleCount.decrementAndGet();
            }
        }

        readMoreBytes(getNextReadBytes());
    }

    //
    // Close Methods
    //

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        executor.submit(() -> {
            synchronized (this) {
                if (null != reconnectTask) {
                    if (reconnectTask.cancel(true)) {
                        log.info("Successfully cancel reconnecting to data range {} @ storage container.",
                            range, scChannel.getStorageContainerId());
                    }
                    reconnectTask = null;
                }
            }

            setLastException(new ObjectClosedException("DataRangeEventSetReader(" + range + ")"));
            if (null != requestObserver) {
                requestObserver.onCompleted();
            }
            closeFuture.complete(null);
        });
    }
}
