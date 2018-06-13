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
import com.google.common.collect.Maps;
import io.grpc.stub.StreamObserver;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.impl.stream.event.PendingWrite;
import org.apache.bookkeeper.clients.impl.stream.event.RangePositionImpl;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.clients.impl.stream.exceptions.UnexpectedException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.stream.proto.storage.SetupWriterRequest;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetResponse;
import org.apache.bookkeeper.stream.proto.storage.WriteRequest;
import org.apache.bookkeeper.stream.proto.storage.WriteResponse;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * A default implementation of {@link StreamRangeEventSetWriter}.
 */
@Slf4j
class StreamRangeEventSetWriterImpl
    extends AbstractAutoAsyncCloseable
    implements StreamRangeEventSetWriter, StreamObserver<WriteResponse> {

    private final RangeId range;
    private final String writerName;
    private final ScheduledExecutorService executor;
    private final StorageContainerChannel scChannel;

    @GuardedBy("pendingWrites")
    private final LinkedList<PendingWrite> pendingWrites;
    @GuardedBy("pendingWrites")
    private final Map<Long, PendingWrite> pendingWriteMap;
    @GuardedBy("this")
    private StreamObserver<WriteRequest> requestObserver;
    @GuardedBy("this")
    private boolean isFenced = false;
    @GuardedBy("this")
    private Throwable error = null;
    @GuardedBy("this")
    private CompletableFuture<StreamRangeEventSetWriter> initFuture = null;

    StreamRangeEventSetWriterImpl(RangeId range,
                                  String writerName,
                                  ScheduledExecutorService executor,
                                  StorageContainerChannel channel) {
        this.range = range;
        this.writerName = writerName;
        this.executor = executor;
        this.scChannel = channel;
        this.pendingWriteMap = Maps.newHashMap();
        this.pendingWrites = new LinkedList<>();
    }

    @VisibleForTesting
    RangeId getStreamRange() {
        return range;
    }

    @VisibleForTesting
    synchronized StreamObserver<WriteRequest> getRequestObserver() {
        return requestObserver;
    }

    @VisibleForTesting
    synchronized Throwable getError() {
        return error;
    }

    @VisibleForTesting
    synchronized int getNumPendingWrites() {
        return pendingWrites.size();
    }

    @VisibleForTesting
    public synchronized boolean isFenced() {
        return isFenced;
    }

    @VisibleForTesting
    synchronized StreamRangeEventSetWriterImpl setRequestObserver(StreamObserver<WriteRequest> requestObserver) {
        this.requestObserver = requestObserver;
        if (null == this.initFuture) {
            this.initFuture = FutureUtils.createFuture();
        }
        this.initFuture.complete(this);
        return this;
    }

    //
    // Methods for Response Handling
    //

    @Override
    public void onNext(WriteResponse response) {
        executor.submit(() -> unsafeProcessResponse(response));
    }

    private void unsafeProcessResponse(WriteResponse response) {
        switch (response.getRespCase()) {
            case SETUP_RESP:
                unsafeProcessSetupResponse(response);
                break;
            case WRITE_RESP:
                unsafeProcessWriteEventSetResponse(response);
                break;
            default:
                log.warn("Received unknown response {}. Ignoring it.", response);
                break;
        }
    }

    private void unsafeProcessSetupResponse(WriteResponse response) {
        if (StatusCode.SUCCESS == response.getCode()) {
            initFuture.complete(this);
            return;
        }
        initFuture.completeExceptionally(
            new InternalStreamException(response.getCode(), "Failed to setup writer on stream range : " + range));
        closeAsync();
    }

    private void completeInitFutureExceptionally(Throwable cause) {
        if (null != initFuture) {
            initFuture.completeExceptionally(cause);
        }
    }

    private void unsafeProcessWriteEventSetResponse(WriteResponse response) {
        if (null == initFuture || !initFuture.isDone()) {
            log.warn("Unexpected: received write response before setting up the writer, closing the writer ...");
            completeInitFutureExceptionally(
                new UnexpectedException(
                    "received write response before setting up the writer, closing the writer ..."));
            closeAsync();
        }

        StatusCode code = response.getCode();
        WriteEventSetResponse writeResponse = response.getWriteResp();

        PendingWrite write;
        synchronized (pendingWrites) {
            write = pendingWriteMap.remove(writeResponse.getEventSetId());
        }
        if (null == write) {
            log.warn("No pending write found for sequence id {} on writing to range {}",
                writeResponse.getEventSetId(), range);
            return;
        }

        if (isClosed()) {
            log.debug("Receiving response after the writer for data range {} is closed.", range);
            return;
        }

        switch (code) {
            case SUCCESS:
                RangePositionImpl pos = RangePositionImpl.create(
                    writeResponse.getRangeId(),
                    writeResponse.getRangeOffset(),
                    writeResponse.getRangeSeqNum()
                );
                write.setRangePosition(pos);
                completePendingWrites();
                break;
            case STREAM_RANGE_FENCED:
                synchronized (this) {
                    isFenced = true;
                }
                failPendingWrites(new InternalStreamException(
                    StatusCode.STREAM_RANGE_FENCED,
                    "Range '" + range + "' is already fenced"
                ));
                closeAsync();
                break;
            case CONDITIONAL_WRITE_FAILURE:
                write.setCause(new InternalStreamException(
                    StatusCode.CONDITIONAL_WRITE_FAILURE,
                    "Conditional write to range '" + range + "' failed"
                ));
                completePendingWrites();
                break;
            default: // all other exceptions, we treat it as write exceptions.
                failPendingWrites(new InternalStreamException(
                    code,
                    "Failed to write range '" + range + "'"));
                closeAsync();
                break;
        }
    }

    private void completePendingWrites() {
        PendingWrite write;
        synchronized (pendingWrites) {
            while ((write = pendingWrites.peek()) != null) {
                if (!write.isDone()) {
                    break;
                }
                write.submitCallback();
                write.getEventSet().release();
                pendingWrites.remove();
                pendingWriteMap.remove(write.getSequenceId());
            }
        }
    }

    private void failPendingWrites(Throwable cause) {
        PendingWrite write;
        synchronized (pendingWrites) {
            while ((write = pendingWrites.peek()) != null) {
                if (!write.isDone()) {
                    write.setCause(cause);
                }
                write.submitCallback();
                write.getEventSet().release();
                pendingWrites.remove();
                pendingWriteMap.remove(write.getSequenceId());
            }
        }
    }

    @Override
    public void onError(Throwable cause) {
        executor.submit(() -> unsafeProcessError(cause));
    }

    private void unsafeProcessError(Throwable cause) {
        completeInitFutureExceptionally(cause);
        synchronized (this) {
            if (null == error) {
                error = cause;
            }
            closeAsync();
        }
    }

    @Override
    public void onCompleted() {
        executor.submit(() -> unsafeProcessCompleted());
    }

    private void unsafeProcessCompleted() {
        completeInitFutureExceptionally(new UnexpectedException("Connection closed before initialization."));
        // if the response stream is closed, close the writer
        closeAsync();
    }

    //
    // Methods for DataRangeEventSetWriter
    //

    @Override
    public CompletableFuture<StreamRangeEventSetWriter> initialize() {
        CompletableFuture<StreamRangeEventSetWriter> future;
        synchronized (this) {
            if (null != initFuture) {
                return initFuture;
            }
            initFuture = future = FutureUtils.createFuture();
        }

        connect(initFuture);
        return future;
    }

    private void connect(CompletableFuture<StreamRangeEventSetWriter> initFuture) {
        scChannel.getStorageContainerChannelFuture().whenCompleteAsync((rsChannel, cause) -> {
            if (null != cause) {
                initFuture.completeExceptionally(cause);
                return;
            }

            StreamObserver<WriteRequest> requestObserver =
                rsChannel.getStreamWriteService().write(this);

            synchronized (this) {
                this.requestObserver = requestObserver;
            }

            requestObserver.onNext(WriteRequest.newBuilder()
                .setSetupReq(SetupWriterRequest.newBuilder()
                    .setStreamId(range.getStreamId())
                    .setRangeId(range.getRangeId())
                    .setWriterName(writerName))
                .build()
            );

        }, executor);
    }

    @Override
    public void write(PendingWrite pendingWrite) throws InternalStreamException {
        synchronized (this) {
            if (isFenced) {
                throw new InternalStreamException(
                    StatusCode.STREAM_RANGE_FENCED,
                    "Range '" + range + "' is already fenced");
            }
            if (null != error) {
                throw new InternalStreamException(
                    StatusCode.FAILURE,
                    "Failed to write events to range '" + range + "'",
                    error);
            }
            if (isClosed()) {
                throw new InternalStreamException(
                    StatusCode.FAILURE,
                    "EventSet writer is already closed for range '" + range + "'",
                    new ObjectClosedException(this.toString()));
            }
        }

        synchronized (pendingWrites) {
            pendingWriteMap.put(pendingWrite.getSequenceId(), pendingWrite);
            pendingWrite.getEventSet().retain();
            pendingWrites.add(pendingWrite);
        }

        synchronized (this) {
            requestObserver.onNext(
                WriteRequest.newBuilder()
                    .setWriteReq(pendingWrite.toWriteEventSetRequest())
                    .build());
        }
    }

    //
    // Methods for AsyncCloseable
    //

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        executor.submit(() -> unsafeCloseAsyncOnce(closeFuture));
    }

    private void unsafeCloseAsyncOnce(CompletableFuture<Void> closeFuture) {
        synchronized (this) {
            requestObserver.onCompleted();
        }

        // fail the pending writes with timeout exception. those pending writes
        // might be outstanding already or never attempted. we treat them in same way
        // as 'timeout'. so the caller can handle and retry them safely.
        Throwable cause;
        synchronized (this) {
            if (null != error) {
                cause = error;
            } else {
                cause = new InternalStreamException(
                    StatusCode.FAILURE,
                    "Timeout on writing events to range '" + range + "'");
            }
        }
        failPendingWrites(cause);

        closeFuture.complete(null);
    }

    @Override
    public String toString() {
        return String.format("StreamRangeEventSetWriter(%s)", range);
    }
}
