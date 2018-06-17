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

package org.apache.bookkeeper.clients.impl.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.stream.FlushResult;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.WriteBatch;
import org.apache.bookkeeper.api.stream.WriteEvent;
import org.apache.bookkeeper.api.stream.WriteEventBuilder;
import org.apache.bookkeeper.api.stream.WriteResult;
import org.apache.bookkeeper.api.stream.Writer;
import org.apache.bookkeeper.api.stream.WriterConfig;
import org.apache.bookkeeper.clients.impl.internal.api.HashStreamRanges;
import org.apache.bookkeeper.clients.impl.internal.api.MetaRangeClient;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.impl.routing.RangeRouter;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet;
import org.apache.bookkeeper.clients.impl.stream.event.PendingEvent;
import org.apache.bookkeeper.clients.impl.stream.event.PendingWrite;
import org.apache.bookkeeper.clients.impl.stream.event.WriteEventBuilderImpl;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeClients;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeEventWriter;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeEventWriterFactory;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeEventWriterFactoryImpl;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.common.util.ExceptionUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * A default implementation of {@link Writer}.
 */
@ThreadSafe
@Slf4j
class WriterImpl<KeyT, ValueT>
    extends AbstractAutoAsyncCloseable
    implements Writer<KeyT, ValueT> {

    private static final String COMPONENT_NAME = WriterImpl.class.getSimpleName();

    private final String writerName;
    private final long streamId;
    private final String streamName;
    private final StreamProperties props;
    private final StreamConfig<KeyT, ValueT> streamConfig;
    private final WriterConfig writerConfig;
    private final MetaRangeClient metaRangeClient;
    private final ScheduledExecutorService executor;
    private final EventSet.ReaderBuilder<KeyT, ValueT> eventSetReaderBuilder;
    private final WriteEventBuilder<KeyT, ValueT> writeEventBuilder;
    private final PendingEvent.Recycler<KeyT, ValueT> pendingEventRecycler;
    private final StreamRangeEventWriterFactory rangeEventWriterFactory;

    // States
    private final RangeRouter<KeyT> rangeRouter;
    private final ConcurrentMap<Long, StreamRangeEventWriter<KeyT, ValueT>> activeWriters =
        new ConcurrentHashMap<>();

    WriterImpl(String writerName,
               String streamName,
               StreamProperties props,
               StreamConfig<KeyT, ValueT> streamConfig,
               WriterConfig writerConfig,
               StorageServerClientManager clientManager,
               OrderedScheduler scheduler) {
        this(
            writerName,
            streamName,
            props,
            streamConfig,
            writerConfig,
            clientManager,
            scheduler,
            new StreamRangeEventWriterFactoryImpl(
                new StreamRangeClients(
                    clientManager.getStorageContainerChannelManager(),
                    scheduler),
                scheduler
            ));
    }

    WriterImpl(String writerName,
               String streamName,
               StreamProperties props,
               StreamConfig<KeyT, ValueT> streamConfig,
               WriterConfig writerConfig,
               StorageServerClientManager clientManager,
               OrderedScheduler scheduler,
               StreamRangeEventWriterFactory rangeEventWriterFactory) {
        this.writerName = writerName;
        this.streamName = streamName;
        this.streamId = props.getStreamId();
        this.props = props;
        this.streamConfig = streamConfig;
        this.writerConfig = writerConfig;
        this.executor = scheduler.chooseThread(props.getStreamId());
        this.rangeRouter = new RangeRouter<>(streamConfig.keyRouter());
        this.eventSetReaderBuilder = EventSet.<KeyT, ValueT>newReaderBuilder()
            .withKeyCoder(streamConfig.keyCoder())
            .withValueCoder(streamConfig.valueCoder());
        this.metaRangeClient = clientManager.openMetaRangeClient(props);
        this.writeEventBuilder = new WriteEventBuilderImpl<>();
        this.pendingEventRecycler = new PendingEvent.Recycler<>();
        this.rangeEventWriterFactory = rangeEventWriterFactory;
    }

    @Override
    public WriteEventBuilder<KeyT, ValueT> eventBuilder() {
        return writeEventBuilder;
    }

    @VisibleForTesting
    RangeRouter<KeyT> getRangeRouter() {
        return rangeRouter;
    }

    @VisibleForTesting
    ConcurrentMap<Long, StreamRangeEventWriter<KeyT, ValueT>> getActiveWriters() {
        return activeWriters;
    }

    @Override
    public CompletableFuture<Writer<KeyT, ValueT>> initialize() {
        return ExceptionUtils.callAndHandleClosedAsync(
            COMPONENT_NAME,
            isClosed(),
            this::setupWriter);
    }

    void setupWriter(CompletableFuture<Writer<KeyT, ValueT>> initFuture) {
        updateLatestStreamRanges().whenCompleteAsync((value, cause) -> {
            if (null == cause) {
                initFuture.complete(WriterImpl.this);
            } else {
                initFuture.completeExceptionally(cause);
            }
        }, executor);
    }

    StreamRangeEventWriter<KeyT, ValueT> newStreamRangeEventWriter(RangeProperties rProps) {
        return rangeEventWriterFactory.createRangeEventWriter(
            writerName,
            streamConfig,
            props,
            rProps,
            writerConfig);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<WriteResult> write(WriteEvent<KeyT, ValueT> event) {
        CompletableFuture<Position> writeFuture = new CompletableFuture<>();
        write(event, writeFuture);
        return writeFuture.thenApply(WriteResultImpl::of);
    }

    public void write(WriteEvent<KeyT, ValueT> event,
                      @Nullable CompletableFuture<Position> future) {
        Long range = rangeRouter.getRange(event.key());
        StreamRangeEventWriter<KeyT, ValueT> eventWriter = activeWriters.get(range);
        while (null == eventWriter) {
            handleStreamRangeFenced();
            // refresh the range again
            range = rangeRouter.getRange(event.key());
            eventWriter = activeWriters.get(range);
        }
        try {
            eventWriter.write(event, future);
        } catch (InternalStreamException e) {
            if (StatusCode.STREAM_RANGE_FENCED == e.getCode()) {
                log.info("Data range {} for stream {} is fenced", range, streamId, e);
                handleStreamRangeFenced();
            } else {
                executor.submit(() ->
                    FutureUtils.completeExceptionally(future, e));
            }
        }
    }

    /**
     * Refetch the latest data ranges and returns the pending writes.
     *
     * <p>Those pending writes will be resent again.
     *
     * @return a list of pending writes that should be resent again.
     */
    CompletableFuture<List<PendingWrite>> updateLatestStreamRanges() {
        CompletableFuture<List<PendingWrite>> updateFuture = FutureUtils.createFuture();
        metaRangeClient.getActiveDataRanges().whenCompleteAsync((ranges, cause) -> {
            if (null != cause) {
                updateFuture.completeExceptionally(cause);
                return;
            }
            refreshStreamRangeEventWriter(updateFuture, ranges);
        }, executor);
        return updateFuture;
    }

    public void refreshStreamRangeEventWriter(CompletableFuture<List<PendingWrite>> result,
                                              HashStreamRanges newRanges) {
        // compare the ranges to see if it requires an update
        HashStreamRanges oldRanges = rangeRouter.getRanges();
        if (null != oldRanges && oldRanges.getMaxRangeId() >= newRanges.getMaxRangeId()) {
            log.info("No new stream ranges found for stream {}.", streamName);
            result.complete(Lists.newArrayList());
            return;
        }
        // update ranges
        if (log.isInfoEnabled()) {
            log.info("Updated the active ranges to {}", newRanges);
        }
        rangeRouter.setRanges(newRanges);
        // add new ranges
        Set<Long> activeRanges = Sets.newHashSetWithExpectedSize(newRanges.getRanges().size());
        newRanges.getRanges().forEach((rk, range) -> {
            activeRanges.add(range.getRangeId());
            if (activeWriters.containsKey(range.getRangeId())) {
                return;
            }
            StreamRangeEventWriter<KeyT, ValueT> writer = newStreamRangeEventWriter(range);
            if (log.isInfoEnabled()) {
                log.info("Create range writer for range {}", range.getRangeId());
            }
            activeWriters.put(range.getRangeId(), writer);
        });
        // remove old ranges
        Iterator<Map.Entry<Long, StreamRangeEventWriter<KeyT, ValueT>>> writerIter =
            activeWriters.entrySet().iterator();
        List<PendingWrite> pendingWrites = Lists.newArrayList();
        while (writerIter.hasNext()) {
            Map.Entry<Long, StreamRangeEventWriter<KeyT, ValueT>> writerEntry = writerIter.next();
            Long rid = writerEntry.getKey();
            if (activeRanges.contains(rid)) {
                continue;
            }
            writerIter.remove();
            StreamRangeEventWriter<KeyT, ValueT> oldWriter = writerEntry.getValue();
            pendingWrites.addAll(oldWriter.closeAndGetPendingWrites());
        }

        // after collecting all the pending writes, complete the refresh process
        result.complete(pendingWrites);
    }

    private void handleStreamRangeFenced() {
        Iterator<PendingWrite> pendingWrites;
        try {
            pendingWrites = FutureUtils.result(updateLatestStreamRanges()).iterator();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<PendingEvent<KeyT, ValueT>> pendingEvents = Lists.newArrayList();
        PendingWrite write;
        while (pendingWrites.hasNext()) {
            write = pendingWrites.next();
            try {
                pendingEvents.addAll(
                    Lists.newArrayList(PendingEvent.iterate(
                        eventSetReaderBuilder,
                        write.getEventSet(),
                        writeEventBuilder,
                        pendingEventRecycler)));
            } finally {
                write.getEventSet().release();
            }
        }

        for (PendingEvent<KeyT, ValueT> event : pendingEvents) {
            try (WriteEvent<KeyT, ValueT> writeEvent = event.getAndReset()) {
                write(writeEvent, event.getFuture());
            }
        }
    }

    @Override
    public CompletableFuture<FlushResult> flush() {
        CompletableFuture<FlushResult> result = FutureUtils.createFuture();
        executor.submit(() -> {
            List<StreamRangeEventWriter<KeyT, ValueT>> writers = Lists.newArrayList(
                activeWriters.values());
            List<CompletableFuture<Void>> flushResults = Lists.newArrayListWithExpectedSize(writers.size());
            for (StreamRangeEventWriter<KeyT, ValueT> writer : writers) {
                flushResults.add(writer.flush());
            }
            FutureUtils.collect(flushResults).whenCompleteAsync((value, cause) -> {
                if (null == cause) {
                    result.complete(FlushResultImpl.of());
                } else {
                    result.completeExceptionally(cause);
                }
            }, executor);
        });
        return result;
    }

    CompletableFuture<Writer<KeyT, ValueT>> flushAndClose() {
        CompletableFuture<Writer<KeyT, ValueT>> result = FutureUtils.createFuture();
        executor.submit(() -> {
            List<StreamRangeEventWriter<KeyT, ValueT>> writers = Lists.newArrayList(
                activeWriters.values());
            List<CompletableFuture<Void>> flushResults = Lists.newArrayListWithExpectedSize(writers.size());
            for (StreamRangeEventWriter<KeyT, ValueT> writer : writers) {
                flushResults.add(writer.flush().thenCompose((value) -> writer.closeAsync()));
            }
            FutureUtils.collect(flushResults).whenCompleteAsync((value, cause) -> {
                if (null == cause) {
                    result.complete(this);
                } else {
                    result.completeExceptionally(cause);
                }
            }, executor);
        });
        return result;
    }

    @Override
    public WriteBatch<KeyT, ValueT> newBatch() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        flushAndClose().whenCompleteAsync((value, cause) -> {
            if (null == cause) {
                closeFuture.complete(null);
            } else {
                closeFuture.completeExceptionally(cause);
            }
        }, executor);
    }

}
