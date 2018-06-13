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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.Code;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.ReadEvent;
import org.apache.bookkeeper.api.stream.ReadEvents;
import org.apache.bookkeeper.api.stream.Reader;
import org.apache.bookkeeper.api.stream.ReaderConfig;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.exceptions.StreamApiException;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet;
import org.apache.bookkeeper.clients.impl.stream.event.EventPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.event.RangesPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.event.ReadEventsImpl;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeClients;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeEventReader;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeEventReaderImpl;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.ParentRanges;
import org.apache.bookkeeper.stream.proto.ParentRangesList;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.ReadPosition;
import org.apache.bookkeeper.stream.proto.StreamPosition;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.readgroup.AddReaderCommand;
import org.apache.bookkeeper.stream.proto.readgroup.CompleteRangeCommand;
import org.apache.bookkeeper.stream.proto.readgroup.ReadGroupAction;
import org.apache.bookkeeper.stream.proto.readgroup.ReadGroupCommand;
import org.apache.bookkeeper.stream.proto.readgroup.ReleaseRangesCommand;
import org.apache.bookkeeper.stream.proto.readgroup.RemoveReaderCommand;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * The default implementation of {@link Reader}.
 */
@Slf4j
class ReaderImpl<KeyT, ValueT>
    extends AbstractAutoAsyncCloseable
    implements Reader<KeyT, ValueT>, StreamObserver<ReadGroupAction> {

    private enum State {
        UNINIT,
        ADDING,
        ADDED,
        REMOVING,
        REMOVED,
        CLOSE
    }

    @Data(staticConstructor = "of")
    private static class ControlReadEvents<KeyT, ValueT> implements ReadEvents<KeyT, ValueT> {

        private final RangeId rangeId;

        public RangeId getRangeId() {
            return rangeId;
        }

        @Override
        public ReadEvent<KeyT, ValueT> next() {
            throw new NoSuchElementException("End of Stream");
        }

        @Override
        public int numEvents() {
            return 0;
        }

        @Override
        public int getEstimatedSize() {
            return 0;
        }

        @Override
        public void close() {}

    }

    private static class EndOfStreamReadEvents<KeyT, ValueT> extends ControlReadEvents<KeyT, ValueT> {
        EndOfStreamReadEvents(RangeId rangeId) {
            super(rangeId);
        }
    }

    private static class ReleaseStreamReadEvents<KeyT, ValueT> extends ControlReadEvents<KeyT, ValueT> {

        ReleaseStreamReadEvents(RangeId rangeId) {
            super(rangeId);
        }
    }

    private final String readerName;
    private final Map<String, Position> positions;
    private final RangesPositionImpl currentPos;
    private final ReaderConfig readerConfig;
    private final StorageServerClientManager clientManager;
    private final StreamRangeClients streamRangeClients;
    private final EventSet.ReaderBuilder<KeyT, ValueT> eventSetReaderBuilder;
    private final ReadEventsImpl.Recycler<KeyT, ValueT> readEventsRecycler;
    private final OrderedScheduler scheduler;
    private final ScheduledExecutorService executor;
    private final StreamObserver<ReadGroupCommand> readGroupCommandar;
    private final AtomicLong cachedSize;
    private final LinkedBlockingQueue<ReadEvents<KeyT, ValueT>> eventSetQueue;
    private final LinkedList<StreamRangeEventReader<KeyT, ValueT>> readerQueue;
    private final Stopwatch readWatch;

    private State state;
    private Throwable error;
    private long readerRevision;
    private final Map<RangeId, StreamRangeEventReader<KeyT, ValueT>> readers;

    @GuardedBy("this")
    private CompletableFuture<Reader<KeyT, ValueT>> startFuture = null;
    @GuardedBy("this")
    private CompletableFuture<Reader<KeyT, ValueT>> stopFuture = null;

    ReaderImpl(String readerName,
               Map<String, Position> positions,
               StreamConfig<KeyT, ValueT> streamConfig,
               ReaderConfig readerConfig,
               StorageServerClientManager rsClientManager,
               OrderedScheduler scheduler,
               StreamObserver<ReadGroupCommand> readGroupCommandar) {
        this.readerName = readerName;
        this.positions = positions;
        this.readerConfig = readerConfig;
        this.eventSetReaderBuilder = EventSet.<KeyT, ValueT>newReaderBuilder()
            .withKeyCoder(streamConfig.keyCoder())
            .withValueCoder(streamConfig.valueCoder());
        this.readEventsRecycler = new ReadEventsImpl.Recycler<>();
        this.clientManager = rsClientManager;
        this.scheduler = scheduler;
        this.executor = scheduler.chooseThread();
        this.readGroupCommandar = readGroupCommandar;
        this.streamRangeClients = new StreamRangeClients(
            rsClientManager.getStorageContainerChannelManager(), scheduler);

        // initialize the state
        this.readers = Maps.newHashMap();
        this.state = State.UNINIT;
        this.cachedSize = new AtomicLong(0L);
        this.eventSetQueue = new LinkedBlockingQueue<>();
        this.readerQueue = new LinkedList<>();
        this.currentPos = new RangesPositionImpl();
        this.readWatch = Stopwatch.createUnstarted();
        log.info("Constructed reader : name = {}, positions = {}", readerName, positions);
    }

    CompletableFuture<Reader<KeyT, ValueT>> start() {
        CompletableFuture<Reader<KeyT, ValueT>> future;
        synchronized (this) {
            if (null != startFuture) {
                return startFuture;
            }
            startFuture = future = FutureUtils.createFuture();
        }
        executor.submit(() -> {
            if (State.UNINIT != state) {
                future.completeExceptionally(
                    new IllegalStateException("Try to start a reader that was already started."));
                return;
            }
            state = State.ADDING;
            readGroupCommandar.onNext(
                newCommandBuilder(ReadGroupCommand.Op.ADD_READER)
                    .setAddCmd(AddReaderCommand.newBuilder())
                    .build());
        });
        return future;
    }

    CompletableFuture<Reader<KeyT, ValueT>> stop() {
        CompletableFuture<Reader<KeyT, ValueT>> future;
        synchronized (this) {
            if (null != stopFuture) {
                return stopFuture;
            }
            stopFuture = future = FutureUtils.createFuture();
        }
        future.whenCompleteAsync((value, cause) -> {
            if (null != cause) {
                readGroupCommandar.onError(cause);
            } else {
                readGroupCommandar.onCompleted();
            }
        }, executor);
        executor.submit(() -> {
            if (State.ADDED != state) {
                future.completeExceptionally(
                    new IllegalStateException("Try to stop a reader that wasn't started yet."));
                return;
            }
            state = State.REMOVING;
            // the reader is already added, then remove it
            readGroupCommandar.onNext(
                newCommandBuilder(ReadGroupCommand.Op.REM_READER)
                    .setRemoveCmd(RemoveReaderCommand.newBuilder()
                        .setReadPos(getPosition().toReadPosition()))
                    .build());
        });
        return future;
    }

    private ReadGroupCommand.Builder newCommandBuilder(ReadGroupCommand.Op op) {
        return ReadGroupCommand.newBuilder()
            .setOp(op)
            .setReaderName(readerName)
            .setReadGroupId(-1L)
            .setLeaseTtlMs(-1L);
    }

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        executor.submit(() -> {
            state = State.CLOSE;
            if (null == error) {
                stop().whenCompleteAsync((reader, cause) -> closeReadersAndComplete(closeFuture), executor);
            } else {
                closeReadersAndComplete(closeFuture);
            }
        });
    }

    private void closeReadersAndComplete(CompletableFuture<Void> closeFuture) {
        FutureUtils.proxyTo(
            closeReaders(Lists.newArrayList(readers.values())),
            closeFuture
        );
    }

    protected CompletableFuture<Void> closeReaders(List<StreamRangeEventReader<KeyT, ValueT>> readers) {
        List<CompletableFuture<Void>> closeFutures = Lists.transform(
            readers,
            (reader) -> reader.closeAsync());
        return FutureUtils.collect(closeFutures).thenApplyAsync((value) -> null);
    }

    private void updateReaderRevision(long revision) {
        readerRevision = Math.max(readerRevision, revision);
    }

    private Throwable createIllegalStateException(State expected, State actual) {
        return new IllegalStateException("Reader " + readerName + " is expected to be at state "
            + expected + " but at state " + actual);
    }

    //
    // Read Group Action
    //

    @Override
    public void onNext(ReadGroupAction action) {
        executor.submit(() -> unsafeProcessReadGroupAction(action));
    }

    private synchronized void completeStartFuture(Throwable cause) {
        if (null == cause) {
            FutureUtils.complete(startFuture, this);
        } else {
            FutureUtils.completeExceptionally(startFuture, cause);
        }
    }

    private synchronized void completeStopFuture(Throwable cause) {
        if (null == cause) {
            FutureUtils.complete(stopFuture, this);
        } else {
            FutureUtils.completeExceptionally(stopFuture, cause);
        }
    }

    private void unsafeProcessReadGroupAction(ReadGroupAction action) {
        Throwable cause = null;
        switch (action.getCode()) {
            case READER_ADDED:
                // the reader is added to the group.
                if (State.ADDING != state) {
                    cause = createIllegalStateException(State.ADDING, state);
                } else {
                    updateReaderRevision(action.getReaderRevision());
                    onReaderAdded();
                }
                break;
            case READER_REMOVED:
                if (State.REMOVING != state) {
                    cause = createIllegalStateException(State.REMOVING, state);
                } else {
                    updateReaderRevision(action.getReaderRevision());
                    onReaderRemoed();
                }
                break;
            case ACQUIRE_RANGE:
                if (State.ADDED != state) {
                    cause = createIllegalStateException(State.ADDED, state);
                } else {
                    updateReaderRevision(action.getReaderRevision());
                    acquireRanges(action.getAcquireAction().getReadPos());
                }
                break;
            case RELEASE_RANGE:
                if (State.ADDED != state) {
                    cause = createIllegalStateException(State.ADDED, state);
                } else {
                    updateReaderRevision(action.getReaderRevision());
                    releaseRanges(action.getReleaseAction().getRangesList());
                }
                break;
            case READER_EXISTS:
                if (State.ADDING != state) {
                    cause = createIllegalStateException(State.ADDING, state);
                } else {
                    cause = new InternalStreamException(
                        StatusCode.READER_EXISTS,
                        "Reader '" + readerName + "' already exists");
                }
                break;
            case READER_NOT_FOUND:
                if (State.REMOVING != state) {
                    cause = createIllegalStateException(State.REMOVING, state);
                } else {
                    cause = new InternalStreamException(
                        StatusCode.READER_NOT_FOUND,
                        "Reader '" + readerName + "' is not found");
                }
                break;
            case REINIT_REQUIRED:
                cause = new InternalStreamException(
                    StatusCode.READER_REINIT_REQUIRED,
                    "Reader '" + readerName + "' is required to reinit");
                break;
            case POSTION_UPDATED:
                updateReaderRevision(action.getReaderRevision());
                break;
            case NO_ACTION:
                // do nothing
                break;
            default:
                log.info("Received unrecognized action {}, ignoring it.", action);
                break;
        }

        if (null != cause) {
            onError(cause);
        }
    }

    private void onReaderAdded() {
        // when the reader is added.
        state = State.ADDED;
        completeStartFuture(null);
    }

    private void onReaderRemoed() {
        // when the reader is removed.
        state = State.REMOVED;
        completeStopFuture(null);
    }

    private void acquireRanges(ReadPosition readPos) {
        for (StreamPosition streamPos : readPos.getStreamPositionsList()) {
            long streamId = streamPos.getStreamId();
            clientManager.getRootRangeClient().getStream(streamId)
                    .whenCompleteAsync((streamProps, cause) -> {
                        if (null != cause) {
                            error = cause;
                            closeAsync();
                        } else {
                            acquireRanges(streamProps, streamPos);
                        }
                    });
        }
    }

    private void acquireRanges(StreamProperties streamProps, StreamPosition streamPos) {
        // TODO: add all ranges later
        clientManager.openMetaRangeClient(streamProps).getActiveDataRanges().thenAccept(streamRanges -> {
            Map<Long, RangeProperties> ranges = new HashMap<>();
            for (RangeProperties rangeProperties : streamRanges.getRanges().values()) {
                ranges.put(rangeProperties.getRangeId(), rangeProperties);
            }

            for (RangePosition rangePos : streamPos.getRangePositionsList()) {
                RangeProperties rangeProps = ranges.get(rangePos.getRangeId());

                if (null != rangePos) {
                    RangeId rid = RangeId.of(streamProps.getStreamId(), rangePos.getRangeId());
                    StreamRangeEventReaderImpl<KeyT, ValueT> reader =
                        new StreamRangeEventReaderImpl<>(
                            streamRangeClients,
                            streamProps.getStreamName(),
                            rid,
                            rangeProps,
                            rangePos,
                            eventSetReaderBuilder,
                            readEventsRecycler,
                            scheduler.chooseThread(rangePos.getRangeId()));
                    reader.initialize().whenCompleteAsync((r, cause) -> {
                        if (null == cause) {
                            readers.put(rid, r);
                            startRead(r);
                        } else {
                            error = cause;
                            closeAsync();
                        }
                    });
                } else {
                    error = new IllegalStateException("No range properties is found for range("
                        + rangePos.getRangeId() + ")@stream(" + streamProps.getStreamName() + ")");
                    closeAsync();
                }
            }
        }).exceptionally(cause -> {
            error = cause;
            closeAsync();
            return null;
        });
    }

    private void startRead(StreamRangeEventReader<KeyT, ValueT> eventReader) {
        // the reader is already closed
        if (isClosed()) {
            return;
        }

        eventReader.readNext().whenCompleteAsync((iterator, cause) -> {
            if (null == cause) {
                eventSetQueue.add(iterator);
                synchronized (this) {
                    if (cachedSize.addAndGet(iterator.getEstimatedSize()) > readerConfig.maxReadAheadCacheSize()) {
                        // add the reader into reader queue
                        readerQueue.offer(eventReader);
                    } else {
                        startRead(eventReader);
                    }
                }
            } else {
                if (cause instanceof InternalStreamException) {
                    InternalStreamException ise = (InternalStreamException) cause;
                    if (StatusCode.END_OF_STREAM_RANGE == ise.getCode()) {
                        RangeId rid = eventReader.getRangeId();
                        eventSetQueue.add(new EndOfStreamReadEvents<>(rid));
                    } else {
                        error = ise;
                        closeAsync();
                    }
                } else if (cause instanceof ObjectClosedException) {
                    if (readers.remove(eventReader.getRangeId(), eventReader)) {
                        log.info("Reader for data range {} is closed.", eventReader.getRangeId());
                    }
                } else {
                    error = cause;
                    closeAsync();
                }
            }
        }, executor);
    }

    private synchronized void scheduleRead() {
        StreamRangeEventReader<KeyT, ValueT> reader = readerQueue.remove();
        while (null != reader) {
            startRead(reader);
            reader = readerQueue.remove();
        }
    }

    private void handleEndOfDataRange(RangeId rangeId) {
        log.info("Hit the end of data range {}.", rangeId);
        clientManager.openMetaRangeClient(rangeId.getStreamId())
            .thenComposeAsync(mrClient -> {
                // TODO: implement the logic in metadata range
                // mrClient.getChildrenDataRangesAndTheirParents(rangeId.getRangeId())
                return FutureUtils.value(Maps.<RangeProperties, List<Long>>newHashMap());
            }, executor)
            .whenCompleteAsync((childrenAndTheirParents, cause) -> {
                if (null != cause) {
                    // retry it later.
                    eventSetQueue.add(new EndOfStreamReadEvents<>(rangeId));
                    return;
                }
                // process the data ranges
                ParentRangesList.Builder listBuilder = ParentRangesList.newBuilder();
                for (Map.Entry<RangeProperties, List<Long>> entry : childrenAndTheirParents.entrySet()) {
                    listBuilder = listBuilder.addChildRanges(ParentRanges.newBuilder()
                        .setRangeId(entry.getKey().getRangeId())
                        .addAllParentRangeIds(entry.getValue()));
                }
                ReadGroupCommand command = newCommandBuilder(ReadGroupCommand.Op.COMPLETE_RANGE)
                    .setCompleteCmd(CompleteRangeCommand.newBuilder()
                        .setStreamId(rangeId.getStreamId())
                        .setRangeId(rangeId.getRangeId())
                        .setChildRanges(listBuilder))
                    .build();
                readGroupCommandar.onNext(command);

                // remove the event reader
                StreamRangeEventReader<KeyT, ValueT> eventReader = readers.remove(rangeId);
                if (null != eventReader) {
                    eventReader.closeAsync();
                } else {
                    log.warn("No reader is found for range {} when handling end of stream range.");
                    onError(new InternalStreamException(
                        StatusCode.READER_REINIT_REQUIRED,
                        "No reader '" + readerName + "'is found for range '"
                            + rangeId + "' when handling end of stream range"));
                }
            }, executor);
    }

    private void releaseRanges(List<org.apache.bookkeeper.stream.proto.RangeId> rangeIds) {
        List<RangeId> ranges =
            Lists.transform(rangeIds, (rangeId) -> RangeId.of(rangeId.getStreamId(), rangeId.getRangeId()));
        for (RangeId rid : ranges) {
            releaseRange(rid);
        }
    }

    private void releaseRange(RangeId rangeId) {
        // remove the event reader
        StreamRangeEventReader<KeyT, ValueT> eventReader = readers.remove(rangeId);
        if (null == eventReader) {
            eventSetQueue.add(new ReleaseStreamReadEvents<>(rangeId));
            return;
        }
        eventReader.closeAsync().whenCompleteAsync((value, cause) -> {
            if (null != cause) {
                log.error("Error on closing reader on range {}", rangeId, cause);
                onError(new InternalStreamException(
                    StatusCode.READER_REINIT_REQUIRED,
                    "Error on closing reader on range '" + rangeId + "'",
                    cause));
                return;
            }
            // close the reader correctly
            eventSetQueue.add(new ReleaseStreamReadEvents<>(rangeId));
        }, executor);
    }

    @GuardedBy("this")
    private void handleReleaseDataRange(RangeId rid) {
        EventPositionImpl esn = currentPos.getPosition(rid);
        if (null == esn) {
            log.error("No position found for range {} when trying to release it", rid);
            onError(new InternalStreamException(
                StatusCode.READER_REINIT_REQUIRED,
                "No position found for range '" + rid + "' when trying to release it"));
            return;
        }
        long streamId = rid.getStreamId();
        StreamPosition.Builder streamPosBuilder = StreamPosition.newBuilder();
        streamPosBuilder.setStreamId(streamId);
        streamPosBuilder.addRangePositions(RangePosition.newBuilder()
            .setRangeId(esn.getRangeId())
            .setOffset(esn.getRangeOffset())
            .setSeqNum(esn.getRangeSeqNum())
            .setSlotId(esn.getSlotId()));
        ReadPosition.Builder readPosBuilder = ReadPosition.newBuilder();
        readPosBuilder.addStreamPositions(streamPosBuilder);
        ReadGroupCommand command = newCommandBuilder(ReadGroupCommand.Op.RELEASE_RANGE)
            .setReleaseCmd(ReleaseRangesCommand.newBuilder()
                .setReadPos(readPosBuilder))
            .build();
        readGroupCommandar.onNext(command);
    }

    @Override
    public void onError(Throwable t) {
        executor.submit(() -> {
            if (null == error) {
                error = t;
            }
            completeStartFuture(error);
            completeStopFuture(error);
            closeAsync();
        });
    }

    @Override
    public void onCompleted() {
        executor.submit(() -> closeAsync());
    }

    //
    // Read Methods
    //

    @SneakyThrows(InterruptedException.class)
    @Override
    public ReadEvents<KeyT, ValueT> readNext(long waitTime, TimeUnit timeUnit)
        throws StreamApiException  {
        if (null != error) {
            throw new StreamApiException(Code.READER_REINIT_REQUIRED, error);
        }
        if (isClosed()) {
            throw new StreamApiException(Code.READER_REINIT_REQUIRED, new ObjectClosedException("reader-" + readerName));
        }

        readWatch.reset().start();

        do {
            ReadEvents<KeyT, ValueT> eventIter = eventSetQueue.poll(waitTime, timeUnit);
            if (null == eventIter) {
                return null;
            }

            long oldCacheSize = cachedSize.get();
            long newCacheSize = cachedSize.addAndGet(-eventIter.getEstimatedSize());
            if (oldCacheSize >= readerConfig.maxReadAheadCacheSize()
                && newCacheSize < readerConfig.maxReadAheadCacheSize()) {
                scheduleRead();
            }

            if (eventIter instanceof ControlReadEvents) {
                ControlReadEvents<KeyT, ValueT> controlReadEvents = (ControlReadEvents<KeyT, ValueT>) eventIter;
                if (eventIter instanceof ReaderImpl.EndOfStreamReadEvents) {
                    // reach end of stream
                    handleEndOfDataRange(controlReadEvents.getRangeId());
                } else if (eventIter instanceof ReaderImpl.ReleaseStreamReadEvents) {
                    // handle release data range
                    handleReleaseDataRange(controlReadEvents.getRangeId());
                }
            } else {
                ReadEventsImpl<KeyT, ValueT> readEventsImpl = (ReadEventsImpl<KeyT, ValueT>) eventIter;

                // update the position
                currentPos.updatePosition(readEventsImpl.getRangeId(), readEventsImpl.getLastEventPosition());
                // return the iterator
                return eventIter;
            }
            waitTime -= readWatch.elapsed(timeUnit);
        } while (waitTime > 0);

        return null;
    }

    @Override
    public RangesPositionImpl getPosition() {
        return currentPos.snapshot();
    }

}
