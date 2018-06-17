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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.clients.impl.stream.utils.PositionUtils.fromPositionImpls;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.impl.stream.event.EventPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.event.StreamPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.ParentRanges;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.ReadPosition;
import org.apache.bookkeeper.stream.proto.readgroup.AcquireRangeAction;
import org.apache.bookkeeper.stream.proto.readgroup.CompleteRangeCommand;
import org.apache.bookkeeper.stream.proto.readgroup.ReadGroupAction;
import org.apache.bookkeeper.stream.proto.readgroup.ReadGroupCommand;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * A local stream observer that observe streams.
 */
@Slf4j
class LocalReadGroupController implements StreamObserver<ReadGroupCommand> {

    enum State {
        UNINIT,
        INIT,
        ADDING,
        ADDED,
        REMOVING,
        REMOVED,
        CLOSE
    }

    private final String nsName;
    private final Map<String, Position> streams;
    private final ScheduledExecutorService executorService;
    private final StorageServerClientManager clientManager;

    // State
    private StreamObserver<ReadGroupAction> actionObserver;
    private State state;
    private final AtomicLong revision = new AtomicLong(0L);
    private final Map<RangeId, EventPositionImpl> unassignedRanges;
    private final Map<RangeId, EventPositionImpl> assignedRanges;
    private final Map<RangeId, Set<Long>> pendingRanges;

    LocalReadGroupController(String nsName,
                             Map<String, Position> streams,
                             StorageServerClientManager clientManager,
                             ScheduledExecutorService executorService) {
        this.nsName = nsName;
        this.streams = streams;
        this.executorService = executorService;
        this.unassignedRanges = Maps.newHashMap();
        this.assignedRanges = Maps.newHashMap();
        this.pendingRanges = Maps.newHashMap();
        this.clientManager = clientManager;
        this.state = State.UNINIT;
        log.info("Constructed the local read group : collection = {}, streams = {}", nsName, streams);
    }

    @VisibleForTesting
    State getState() {
        return this.state;
    }

    @VisibleForTesting
    void setState(State state) {
        this.state = state;
    }

    @VisibleForTesting
    Map<RangeId, EventPositionImpl> getUnassignedRanges() {
        return unassignedRanges;
    }

    @VisibleForTesting
    Map<RangeId, EventPositionImpl> getAssignedRanges() {
        return assignedRanges;
    }

    @VisibleForTesting
    Map<RangeId, Set<Long>> getPendingRanges() {
        return pendingRanges;
    }

    @VisibleForTesting
    StreamObserver<ReadGroupAction> getActionObserver() {
        return actionObserver;
    }

    @VisibleForTesting
    long getRevision() {
        return revision.get();
    }

    @VisibleForTesting
    void setRevision(long newRevision) {
        revision.set(newRevision);
    }

    public CompletableFuture<Void> start(StreamObserver<ReadGroupAction> actionObserver) {
        List<CompletableFuture<Map<RangeId, EventPositionImpl>>> futures =
            Lists.newArrayListWithExpectedSize(streams.size());
        for (Map.Entry<String, Position> stream : streams.entrySet()) {
            futures.add(startStream(stream.getKey(), stream.getValue()));
        }
        return FutureUtils.collect(futures).thenApplyAsync(rangePositions -> {
            for (Map<RangeId, EventPositionImpl> rangePosMap : rangePositions) {
                unassignedRanges.putAll(rangePosMap);
            }
            this.actionObserver = actionObserver;
            this.state = State.INIT;
            return null;
        }, executorService);
    }

    private CompletableFuture<Map<RangeId, EventPositionImpl>> startStream(String stream,
                                                                           Position position) {
        if (Position.TAIL == position) {
            // TODO: add a rpc to retrieve tail position
            return clientManager.getRootRangeClient()
                .getStream(nsName, stream)
                .thenComposeAsync(streamProps -> clientManager.openMetaRangeClient(streamProps)
                    .getActiveDataRanges()
                    .thenApply(streamRanges -> {
                        Map<RangeId, EventPositionImpl> positionMap = new HashMap<>();
                        for (RangeProperties rangeProps : streamRanges.getRanges().values()) {
                            RangeId rid = RangeId.of(streamProps.getStreamId(), rangeProps.getRangeId());
                            EventPositionImpl pos = EventPositionImpl.of(
                                rangeProps.getRangeId(),
                                0,
                                0,
                                0);
                            positionMap.put(rid, pos);
                        }
                        return positionMap;
                    }), executorService);
        } else if (Position.HEAD == position) {
            return FutureUtils.exception(
                new UnsupportedOperationException("Start stream from HEAD is not supported yet"));
        } else if (position instanceof StreamPositionImpl) {
            StreamPositionImpl streamPosImpl = (StreamPositionImpl) position;
            Map<RangeId, EventPositionImpl> positionMap = new HashMap<>();
            for (Map.Entry<Long, EventPositionImpl> posEntry : streamPosImpl.getRangePositions().entrySet()) {
                checkArgument(posEntry.getValue() instanceof EventPositionImpl);
                positionMap.put(
                    RangeId.of(streamPosImpl.getStreamId(), posEntry.getKey()),
                    posEntry.getValue());
            }
            return FutureUtils.value(positionMap);
        } else if (position instanceof EventPositionImpl) {
            EventPositionImpl posImpl = (EventPositionImpl) position;
            return clientManager.getRootRangeClient()
                .getStream(nsName, stream)
                .thenApply(streamProperties -> {
                    Map<RangeId, EventPositionImpl> positionMap = new HashMap<>();
                    positionMap.put(
                        RangeId.of(streamProperties.getStreamId(), posImpl.getRangeId()),
                        posImpl);
                    return positionMap;
                });
        } else {
            return FutureUtils.exception(
                new IllegalArgumentException("Unknown position to read events from stream " + stream));
        }
    }

    @Override
    public void onNext(ReadGroupCommand command) {
        executorService.submit(() -> unsafeProcessReadGroupCommand(command));
    }

    // process the control commands at the executor
    void unsafeProcessReadGroupCommand(ReadGroupCommand command) {
        switch (command.getOp()) {
            case ADD_READER:
                if (State.INIT != state
                    || revision.get() != command.getReaderRevision()) {
                    Throwable cause = new InternalStreamException(
                        StatusCode.UNEXPECTED,
                        "Attempt to add reader while the read group is in unexpected state : state = "
                            + state + ", revision = " + revision + ", cmd = " + command + ".");
                    cause.fillInStackTrace();
                    sendReinitAction(cause);
                } else {
                    sendAction(ReadGroupAction.Code.READER_ADDED);
                    onReaderAdded();
                }
                break;
            case COMPLETE_RANGE:
                if (State.ADDED != state
                    || revision.get() != command.getReaderRevision()) {
                    Throwable cause = new InternalStreamException(
                        StatusCode.UNEXPECTED,
                        "Attempt to complete range while the read group is in unexpected state : state = "
                            + state + ", revision = " + revision + ", cmd = " + command + ".");
                    cause.fillInStackTrace();
                    sendReinitAction(cause);
                } else {
                    onRangeCompleted(command.getCompleteCmd());
                }
                break;
            case REM_READER:
                if (State.ADDED != state
                    || revision.get() != command.getReaderRevision()) {
                    Throwable cause = new InternalStreamException(
                        StatusCode.UNEXPECTED,
                        "Attempt to remove reader while the read group is in unexpected state : state = "
                            + state + ", revision = " + revision + ", cmd = " + command + ".");
                    cause.fillInStackTrace();
                    sendReinitAction(cause);
                } else {
                    onReaderRemoved();
                    sendAction(ReadGroupAction.Code.READER_REMOVED);
                }
                break;
            case UPDATE_POS:
                if (State.ADDED != state
                    || revision.get() != command.getReaderRevision()) {
                    Throwable cause = new InternalStreamException(
                        StatusCode.UNEXPECTED,
                        "Attempt to update pos while the read group is in unexpected state : state = "
                            + state + ", revision = " + revision + ", cmd = " + command + ".");
                    cause.fillInStackTrace();
                    sendReinitAction(cause);
                } else {
                    // TODO: add the ability to update position
                    sendAction(ReadGroupAction.Code.POSTION_UPDATED);
                }
                break;
            case NO_COMMAND:
                // do nothing
                break;
            case ACQUIRE_RANGE:
            case RELEASE_RANGE:
                // no expect to receive these two commands
                Throwable cause = new InternalStreamException(
                    StatusCode.UNEXPECTED,
                    "Received unexpected command in local read group : " + command);
                cause.fillInStackTrace();
                sendReinitAction(cause);
                break;
            default:
                log.info("Received unrecoganized command {}, ignoring it.", command);
                break;
        }
    }

    private void onRangeCompleted(CompleteRangeCommand cmd) {
        RangeId rangeId = RangeId.of(cmd.getStreamId(), cmd.getRangeId());
        EventPositionImpl esn = assignedRanges.remove(rangeId);
        if (null == esn) {
            log.warn("Try to complete reading a not-found range {}.", rangeId);
            Throwable cause = new InternalStreamException(
                StatusCode.UNEXPECTED,
                "Try to complete reading range " + rangeId + " but it is not found in the assigned list");
            cause.fillInStackTrace();
            sendReinitAction(cause);
            return;
        }

        revision.incrementAndGet();
        // add the completed range's children and their parents into the 'pendingRanges'
        //    we need this structure because:
        //    - a child range can only be read after all its parents are completed on reading. otherwise we will break
        //      the ordering guarantee.
        for (ParentRanges parentRanges : cmd.getChildRanges().getChildRangesList()) {
            rangeId = RangeId.of(cmd.getStreamId(), parentRanges.getRangeId());
            if (pendingRanges.containsKey(rangeId)) { // the range and its parent already exist
                continue;
            }
            Set<Long> parent = Sets.newHashSet();
            parent.addAll(parentRanges.getParentRangeIdsList());
            pendingRanges.put(rangeId, parent);
        }

        // delete the completed range from the parent range map
        Iterator<Entry<RangeId, Set<Long>>> iter = pendingRanges.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<RangeId, Set<Long>> rangeAndParents = iter.next();
            Set<Long> parents = rangeAndParents.getValue();
            parents.remove(cmd.getRangeId());
            if (parents.isEmpty()) {
                // all its parents are completed now.
                unassignedRanges.put(
                    rangeAndParents.getKey(),
                    EventPositionImpl.createInitPos(rangeAndParents.getKey().getRangeId()));
                iter.remove();
            }
        }

        if (unassignedRanges.isEmpty()) {
            sendAction(ReadGroupAction.Code.POSTION_UPDATED);
        } else {
            ReadPosition readPos = fromPositionImpls(unassignedRanges);
            assignedRanges.putAll(unassignedRanges);
            unassignedRanges.clear();
            sendAcquireAction(readPos);
        }
    }

    private void onReaderAdded() {
        state = State.ADDING;

        revision.incrementAndGet();
        ReadPosition readPos = fromPositionImpls(unassignedRanges);
        assignedRanges.putAll(unassignedRanges);
        unassignedRanges.clear();
        sendAcquireAction(readPos);

        state = State.ADDED;
    }

    private void onReaderRemoved() {
        state = State.REMOVING;

        revision.incrementAndGet();

        state = State.REMOVED;
    }

    private void sendAcquireAction(ReadPosition pos) {
        if (null == actionObserver) {
            return;
        }
        actionObserver.onNext(ReadGroupAction.newBuilder()
            .setCode(ReadGroupAction.Code.ACQUIRE_RANGE)
            .setReaderRevision(revision.get())
            .setAcquireAction(AcquireRangeAction.newBuilder()
                .setReadPos(pos))
            .build());
    }

    private void sendAction(ReadGroupAction.Code code) {
        if (null == actionObserver) {
            return;
        }
        actionObserver.onNext(ReadGroupAction.newBuilder()
            .setCode(code)
            .setReaderRevision(revision.get())
            .build());
    }

    private void sendReinitAction(Throwable cause) {
        if (State.CLOSE == state) {
            return;
        }

        log.info("Reader reinit is required by the local read group : state = {}, revision = {}",
            state, revision.get(), cause);

        state = State.CLOSE;
        if (null == actionObserver) {
            return;
        }
        actionObserver.onNext(ReadGroupAction.newBuilder()
            .setCode(ReadGroupAction.Code.REINIT_REQUIRED)
            .setReaderRevision(revision.get())
            .build());
    }

    @Override
    public void onError(Throwable t) {
        executorService.submit(() -> sendReinitAction(t));
    }

    @Override
    public void onCompleted() {
        executorService.submit(() -> {
            state = State.CLOSE;
        });
    }
}
