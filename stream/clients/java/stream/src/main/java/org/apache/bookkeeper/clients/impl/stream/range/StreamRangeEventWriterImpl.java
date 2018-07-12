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

import static org.apache.bookkeeper.clients.utils.ClientConstants.DEFAULT_BACKOFF_START_MS;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_SEQUENCE_ID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.WriteEvent;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet.WriterBuilder;
import org.apache.bookkeeper.clients.impl.stream.event.PendingEventSet;
import org.apache.bookkeeper.clients.impl.stream.event.PendingWrite;
import org.apache.bookkeeper.clients.impl.stream.event.RangePositionImpl;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * An implementation for {@link StreamRangeEventSetWriter}.
 */
@Slf4j
public class StreamRangeEventWriterImpl<KeyT, ValueT>
    extends AbstractAutoAsyncCloseable
    implements StreamRangeEventWriter<KeyT, ValueT>, Runnable {

    /**
     * Event Writer State.
     */
    @VisibleForTesting
    enum State {
        INITIALIZING,
        INITIALIZED,
        FENCED,
        CLOSING,
        CLOSED
    }

    //
    // Settings
    //

    private final StreamRangeClients streamRangeClients;
    private final RangeProperties rangeProps;
    private final RangeId rangeId;
    private final String writerName;
    private final WriterBuilder<KeyT, ValueT> writerBuilder;
    private final ScheduledExecutorService executor;
    private final ScheduledFuture<?> flushTask;

    //
    // Variables tracks the status of this writer
    //

    @GuardedBy("this")
    private EventSet.Writer<KeyT, ValueT> currentEventSetWriter;
    @GuardedBy("this")
    private List<CompletableFuture<Position>> currentPendingCallbacks;
    @GuardedBy("this")
    private State state = State.INITIALIZING;

    // unsafe variables (should be modified in executor)
    private long numPendingEvents = 0L;
    private long numPendingEventSets = 0L;
    private long numPendingBytes = 0L;
    private LinkedList<PendingWrite> unackedWrites;
    private LinkedList<PendingWrite> pendingWrites;
    private StreamRangeEventSetWriter rangeWriter;
    private long lastSequencedIdPushed = INVALID_SEQUENCE_ID;
    private long lastSequencedIdAcked = INVALID_SEQUENCE_ID;

    public StreamRangeEventWriterImpl(StreamRangeClients streamRangeClients,
                                      long streamId,
                                      RangeProperties rangeProps,
                                      String writerName,
                                      WriterBuilder<KeyT, ValueT> writerBuidler,
                                      Duration flushDuration,
                                      ScheduledExecutorService executor) {
        this.streamRangeClients = streamRangeClients;
        this.rangeProps = rangeProps;
        this.writerName = writerName;
        this.rangeId = RangeId.of(streamId, rangeProps.getRangeId());
        this.writerBuilder = writerBuidler;
        this.executor = executor;
        this.writerBuilder.withWriterId(0);

        // initialize the first eventset
        this.currentEventSetWriter = newEventSetWriter();
        this.currentPendingCallbacks = Lists.newArrayList();
        this.pendingWrites = Lists.newLinkedList();
        this.unackedWrites = Lists.newLinkedList();

        // open the writer
        openRangeWriterLater(0L);

        // schedule the periodical flush task
        long flushIntervalInNanos = flushDuration.toNanos();
        if (flushIntervalInNanos > 0) {
            flushTask = executor.scheduleAtFixedRate(
                this,
                flushIntervalInNanos,
                flushIntervalInNanos,
                TimeUnit.NANOSECONDS);
        } else {
            flushTask = null;
        }
    }

    private EventSet.Writer<KeyT, ValueT> newEventSetWriter() {
        return writerBuilder.build();
    }

    @VisibleForTesting
    ScheduledFuture<?> getFlushTask() {
        return flushTask;
    }

    @VisibleForTesting
    long getNumPendingEvents() {
        return numPendingEvents;
    }

    @VisibleForTesting
    long getNumPendingEventSets() {
        return numPendingEventSets;
    }

    @VisibleForTesting
    long getNumPendingBytes() {
        return numPendingBytes;
    }

    @VisibleForTesting
    List<PendingWrite> getPendingWrites() {
        return pendingWrites;
    }

    @VisibleForTesting
    List<PendingWrite> getUnackedWrites() {
        return unackedWrites;
    }

    //
    // State Management
    //

    @VisibleForTesting
    synchronized State getState() {
        return state;
    }

    private synchronized boolean isInClosingState() {
        return State.CLOSING == state || State.CLOSED == state;
    }

    private synchronized boolean isInFencedState() {
        return State.FENCED == state;
    }

    private synchronized boolean isInInitializedState() {
        return State.INITIALIZED == state;
    }

    private synchronized boolean isInInitializingOrInitializedState() {
        return State.INITIALIZED == state || State.INITIALIZING == state;
    }

    private synchronized void setStateToFenced() {
        state = State.FENCED;
    }

    private synchronized boolean setStateToInitializing() {
        if (State.INITIALIZED == state
            || State.INITIALIZING == state) {
            state = State.INITIALIZING;
            if (log.isDebugEnabled()) {
                log.debug("Set range writer ({}) to {}", rangeId, state);
            }
            return true;
        } else {
            return false;
        }
    }

    private synchronized void setStateToInitialized() {
        state = State.INITIALIZED;
        if (log.isDebugEnabled()) {
            log.debug("Set range writer ({}) to {}", rangeId, state);
        }
    }

    //
    // Flush Logic
    //

    @Override
    public CompletableFuture<Void> flush() {
        ByteBuf eventSetBufToFlush;
        List<CompletableFuture<Position>> callbacksToFlush;
        synchronized (this) {
            if (currentEventSetWriter.getNumEvents() == 0) {
                return FutureUtils.value(null);
            }
            eventSetBufToFlush = currentEventSetWriter.complete();
            callbacksToFlush = currentPendingCallbacks;
            currentEventSetWriter = newEventSetWriter();
            currentPendingCallbacks = Lists.newArrayList();
        }
        return transmit(eventSetBufToFlush, callbacksToFlush).thenApply((value) -> null);
    }

    private CompletableFuture<RangePositionImpl> transmit(ByteBuf eventSetBuf,
                                                          List<CompletableFuture<Position>> callbacks) {
        PendingEventSet eventSet = PendingEventSet.of(
            eventSetBuf,
            callbacks);
        CompletableFuture<RangePositionImpl> transmitFuture = FutureUtils.createFuture();
        executor.submit(() -> unsafeTransmit(eventSet, transmitFuture));
        return transmitFuture;
    }

    private void unsafeTransmit(PendingEventSet eventSet,
                                CompletableFuture<RangePositionImpl> transmitFuture) {
        long sequenceId = lastSequencedIdPushed + 1;
        PendingWrite write = PendingWrite.of(
            sequenceId,
            eventSet,
            transmitFuture);
        ++lastSequencedIdPushed;
        ++numPendingEventSets;
        numPendingEvents += eventSet.getCallbacks().size();
        numPendingBytes += eventSet.getEventSetBuf().readableBytes();

        unsafeWriteEventSet(write);
    }

    @Override
    public void run() {
        if (isClosed()) {
            return;
        }
        flush();
    }

    //
    // Writer Interface
    //

    private void completeWriteExceptionallyInExecutor(@Nullable CompletableFuture<Position> future,
                                                      Throwable cause) {
        if (null == future) {
            return;
        }
        executor.submit(() -> future.completeExceptionally(cause));
    }

    @Override
    public synchronized void write(WriteEvent<KeyT, ValueT> event,
                                   @Nullable CompletableFuture<Position> future) throws InternalStreamException  {
        try {
            if (isInFencedState()) {
                throw new InternalStreamException(
                    StatusCode.STREAM_RANGE_FENCED,
                    "Range '" + rangeId + "' is already fenced");
            }

            if (isInClosingState()) {
                throw new InternalStreamException(
                    StatusCode.FAILURE,
                    "Event is cancelled before writing to range '" + rangeId + "'");
            }

            boolean added = currentEventSetWriter.writeEvent(
                event.key(),
                event.value(),
                event.timestamp());
            if (!added) {
                flush();
                added = currentEventSetWriter.writeEvent(
                    event.key(),
                    event.value(),
                    event.timestamp());
                if (!added) {
                    if (null != future) {
                        completeWriteExceptionallyInExecutor(
                            future,
                            new InternalStreamException(
                                StatusCode.INVALID_EVENT,
                                "Invalid event to append"
                            ));
                        return;
                    } else {
                        log.error("Invalid event (key = {}, value = {}, timestamp = {}) to write.",
                            event.key(), event.value(), event.timestamp());
                        return;
                    }
                }
            }
            currentPendingCallbacks.add(future);
        } finally {
            event.close();
        }
    }

    private void unsafeWriteEventSet(PendingWrite pendingWrite) {
        if (isInInitializedState()) {
            try {
                rangeWriter.write(pendingWrite);
                unackedWrites.add(pendingWrite);
                pendingWrite
                    .getWriteFuture()
                    .whenCompleteAsync((sequence, throwable) -> {
                        if (log.isTraceEnabled()) {
                            log.trace("Received response {} for write {} - {}",
                                sequence, pendingWrite, throwable);
                        }
                        unsafeProcessWriteEventSetResult();
                    }, executor);
            } catch (InternalStreamException e) {
                if (StatusCode.STREAM_RANGE_FENCED == e.getCode()) {
                    setStateToFenced();
                    pendingWrites.add(pendingWrite);
                } else {
                    pendingWrites.add(pendingWrite);
                    unsafeOpenRangeWriter();
                }
            }
        } else {
            // the writer is not initialized yet.
            pendingWrites.add(pendingWrite);
        }
    }

    private void unsafeProcessWriteEventSetResult() {
        PendingWrite write = unackedWrites.peek();
        while (null != write) {
            if (!write.isDone()) {
                return;
            }
            if (write.isSuccess()) {
                unackedWrites.poll();
                lastSequencedIdAcked = write.getSequenceId();
                afterPendingWriteRemoved(write);
                write.getEventSet().complete(write.getRangePosition());
            } else {
                boolean isStreamRangeFenced = false;
                if (write.getCause() instanceof InternalStreamException) {
                    InternalStreamException ise = (InternalStreamException) write.getCause();
                    isStreamRangeFenced = (StatusCode.STREAM_RANGE_FENCED == ise.getCode());
                }

                if (isStreamRangeFenced) {
                    setStateToFenced();
                    // we complete the write exceptionally but we don't dequeue it from unacked writes
                    // so the write can be retried again.
                    write.getEventSet().completeExceptionally(write.getCause());
                    return;
                } else {
                    unsafeOpenRangeWriter();
                    return;
                }
            }
            write = unackedWrites.peek();
        }
    }

    private void afterPendingWriteRemoved(PendingWrite write) {
        numPendingEvents -= write.getEventSet().getCallbacks().size();
        numPendingBytes -= write.getEventSet().getEventSetBuf().readableBytes();
        --numPendingEventSets;
    }

    @VisibleForTesting
    void openRangeWriterLater(long delayMs) {
        executor.schedule(() -> {
            unsafeOpenRangeWriter();
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void unsafeOpenRangeWriter() {
        if (!setStateToInitializing()) {
            return;
        }
        if (null == pendingWrites) {
            pendingWrites = Lists.newLinkedList();
        }

        streamRangeClients
            .openStreamRangeWriter(rangeId.getStreamId(), rangeProps, writerName)
            .whenCompleteAsync((rangeWriter, cause) -> {
                if (null != cause) {
                    log.info("Encountering exception on setting up eventset writer for range {} : {}."
                            + " Retrying in {} ms",
                        rangeId, cause.getMessage(), DEFAULT_BACKOFF_START_MS);
                    openRangeWriterLater(DEFAULT_BACKOFF_START_MS);
                    return;
                }
                if (isInClosingState()) {
                    // the writer is in closing state, skip
                    return;
                }
                log.info("Open writer to write data range {}.", rangeId);
                StreamRangeEventWriterImpl.this.rangeWriter = rangeWriter;
                setStateToInitialized();
                List<PendingWrite> unackedWritesToResend = Lists.newArrayListWithExpectedSize(unackedWrites.size());
                for (PendingWrite write : unackedWrites) {
                    unackedWritesToResend.add(PendingWrite.of(write, FutureUtils.createFuture()));
                    write.getEventSet().release();
                }
                unackedWrites.clear();
                int size = unackedWrites.size();
                if (null != pendingWrites) {
                    size += pendingWrites.size();
                }
                List<PendingWrite> writesToSend = Lists.newArrayListWithExpectedSize(size);
                writesToSend.addAll(unackedWritesToResend);
                if (null != pendingWrites) {
                    writesToSend.addAll(pendingWrites);
                    pendingWrites = null;
                }
                writesToSend.forEach(this::unsafeWriteEventSet);
            }, executor);
    }

    @Override
    public List<PendingWrite> closeAndGetPendingWrites() {
        final List<PendingWrite> allPendingWrites = Lists.newArrayList();
        try {
            closeAsync().whenCompleteAsync((result, cause) -> {
                synchronized (allPendingWrites) {
                    allPendingWrites.addAll(unackedWrites);
                    unackedWrites.forEach(this::afterPendingWriteRemoved);
                    unackedWrites.clear();
                    if (null != pendingWrites) {
                        allPendingWrites.addAll(pendingWrites);
                        pendingWrites.forEach(this::afterPendingWriteRemoved);
                        pendingWrites.clear();
                        pendingWrites = null;
                    }
                }
            }, executor).get();
        } catch (InterruptedException e) {
            // no-op. close doesn't fail here
        } catch (ExecutionException e) {
            // no-op. close doesn't fail here
        }
        return allPendingWrites;
    }

    //
    // Closeable Methods
    //

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        executor.submit(() -> {
            synchronized (this) {
                if (isInInitializingOrInitializedState()) {
                    state = State.CLOSING;
                }
            }
            flush();
            if (null != flushTask) {
                flushTask.cancel(true);
            }
            synchronized (this) {
                if (State.CLOSING == state) {
                    state = State.CLOSED;
                }
            }
            executor.submit(() -> {
                closeFuture.complete(null);
            });
        });
    }

}
