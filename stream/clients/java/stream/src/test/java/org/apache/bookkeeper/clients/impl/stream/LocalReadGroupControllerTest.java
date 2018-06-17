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

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.clients.impl.internal.api.HashStreamRanges;
import org.apache.bookkeeper.clients.impl.internal.api.MetaRangeClient;
import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.impl.stream.event.EventPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.event.StreamPositionImpl;
import org.apache.bookkeeper.clients.impl.stream.utils.PositionUtils;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.ParentRanges;
import org.apache.bookkeeper.stream.proto.ParentRangesList;
import org.apache.bookkeeper.stream.proto.RangeKeyType;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.readgroup.AcquireRangeAction;
import org.apache.bookkeeper.stream.proto.readgroup.AddReaderCommand;
import org.apache.bookkeeper.stream.proto.readgroup.CompleteRangeCommand;
import org.apache.bookkeeper.stream.proto.readgroup.ReadGroupAction;
import org.apache.bookkeeper.stream.proto.readgroup.ReadGroupCommand;
import org.apache.bookkeeper.stream.proto.readgroup.RemoveReaderCommand;
import org.apache.bookkeeper.stream.proto.readgroup.UpdatePositionCommand;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.apache.bookkeeper.stream.protocol.util.ProtoUtils;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link LocalReadGroupController}.
 */
@Slf4j
public class LocalReadGroupControllerTest {

    private final String nsName = "test-collection";
    private final String streamName1 = "stream-1";
    private final String streamName2 = "stream-2";
    private final long streamId1 = 1234L;
    private final long streamId2 = 1235L;
    private final StreamProperties streamProps1 = StreamProperties.newBuilder()
        .setStreamId(streamId1)
        .setStorageContainerId(1L)
        .setStreamName(streamName1)
        .setStreamConf(DEFAULT_STREAM_CONF)
        .build();
    private final StreamProperties streamProps2 = StreamProperties.newBuilder()
        .setStreamId(streamId2)
        .setStorageContainerId(2L)
        .setStreamName(streamName2)
        .setStreamConf(DEFAULT_STREAM_CONF)
        .build();
    private final RootRangeClient rootRangeClient = mock(RootRangeClient.class);
    private final MetaRangeClient metaRangeClient1 = mock(MetaRangeClient.class);
    private final MetaRangeClient metaRangeClient2 = mock(MetaRangeClient.class);
    private final StorageServerClientManager clientManager = mock(StorageServerClientManager.class);
    private final StreamObserver<ReadGroupAction> actionObserver = mockStreamObserver();
    private ScheduledExecutorService executor;

    private static Map<RangeId, EventPositionImpl> prepareRanges(long streamId, int numRanges) {
        Map<RangeId, EventPositionImpl> map = Maps.newHashMap();
        for (int i = 0; i < numRanges; i++) {
            RangeId rangeId = RangeId.of(streamId, i);
            EventPositionImpl esn = EventPositionImpl.of(
                rangeId.getRangeId(),
                ThreadLocalRandom.current().nextLong(1024L),
                ThreadLocalRandom.current().nextLong(1024L),
                ThreadLocalRandom.current().nextInt(1024));
            map.put(rangeId, esn);
        }
        return map;
    }

    private static StreamPositionImpl prepareStreamPosition(long streamId, int numRanges) {
        Map<RangeId, EventPositionImpl> map = prepareRanges(streamId, numRanges);
        StreamPositionImpl pos = new StreamPositionImpl(streamId);
        map.values().forEach(pos::addPosition);
        return pos;
    }

    @SuppressWarnings("unchecked")
    private static <T> StreamObserver<T> mockStreamObserver() {
        return mock(StreamObserver.class);
    }

    @Before
    public void setUp() {
        this.executor = Executors.newSingleThreadScheduledExecutor();
        when(clientManager.getRootRangeClient()).thenReturn(rootRangeClient);
        when(rootRangeClient.getStream(eq(nsName), eq(streamName1))).thenReturn(FutureUtils.value(streamProps1));
        when(rootRangeClient.getStream(eq(nsName), eq(streamName2))).thenReturn(FutureUtils.value(streamProps2));
        when(clientManager.openMetaRangeClient(eq(streamProps1))).thenReturn(metaRangeClient1);
        when(clientManager.openMetaRangeClient(eq(streamProps2))).thenReturn(metaRangeClient2);
    }

    @After
    public void tearDown() {
        if (null != executor) {
            executor.shutdown();
        }
    }

    private LocalReadGroupController createLocalReadGroup(Map<String, Position> streams) {
        return new LocalReadGroupController(
            nsName,
            streams,
            clientManager,
            executor);
    }

    /**
     * TestCase: Start with `Position.HEAD` is not supported.
     */
    @Test
    public void testStartWithPositionHead() {
        Map<String, Position> streamPositions = Maps.newHashMap();
        streamPositions.put(streamName1, Position.HEAD);

        LocalReadGroupController lrg = createLocalReadGroup(streamPositions);
        try {
            FutureUtils.result(lrg.start(actionObserver));
            fail("Should fail to start when failed to fetch the stream position");
        } catch (Exception e) {
            // expected
            assertTrue(e instanceof UnsupportedOperationException);
        }
        assertNull(lrg.getActionObserver());
        assertTrue(lrg.getUnassignedRanges().isEmpty());
        assertTrue(lrg.getAssignedRanges().isEmpty());
        assertTrue(lrg.getPendingRanges().isEmpty());
        assertEquals(LocalReadGroupController.State.UNINIT, lrg.getState());
    }

    /**
     * TestCase: Start with unknown `Position` implementation is not supported.
     */
    @Test
    public void testStartWithPositionUnknown() {
        Map<String, Position> streamPositions = Maps.newHashMap();
        streamPositions.put(streamName1, mock(Position.class));

        LocalReadGroupController lrg = createLocalReadGroup(streamPositions);
        try {
            FutureUtils.result(lrg.start(actionObserver));
            fail("Should fail to start when failed to fetch the stream position");
        } catch (Exception e) {
            // expected
            assertTrue(e instanceof IllegalArgumentException);
        }
        assertNull(lrg.getActionObserver());
        assertTrue(lrg.getUnassignedRanges().isEmpty());
        assertTrue(lrg.getAssignedRanges().isEmpty());
        assertTrue(lrg.getPendingRanges().isEmpty());
        assertEquals(LocalReadGroupController.State.UNINIT, lrg.getState());
    }

    /**
     * TestCase: Start with an event position.
     */
    @Test
    public void testStartWithEventPositionImpl() throws Exception {
        EventPositionImpl eventPos1 = EventPositionImpl.of(
            100,
            1000,
            10000,
            1
        );
        EventPositionImpl eventPos2 = EventPositionImpl.of(
            2 * 100,
            2 * 1000,
            2 * 10000,
            2
        );

        Map<String, Position> streamPositions = Maps.newHashMap();
        streamPositions.put(streamName1, eventPos1);
        streamPositions.put(streamName2, eventPos2);

        LocalReadGroupController lrg = createLocalReadGroup(streamPositions);
        FutureUtils.result(lrg.start(actionObserver));

        assertSame(actionObserver, lrg.getActionObserver());
        assertEquals(LocalReadGroupController.State.INIT, lrg.getState());
        assertTrue(lrg.getAssignedRanges().isEmpty());
        assertTrue(lrg.getPendingRanges().isEmpty());
        Map<RangeId, EventPositionImpl> union = Maps.newHashMap();
        union.put(RangeId.of(streamId1, eventPos1.getRangeId()), eventPos1);
        union.put(RangeId.of(streamId2, eventPos2.getRangeId()), eventPos2);
        MapDifference<RangeId, Position> diff = Maps.difference(union, lrg.getUnassignedRanges());
        assertTrue(diff.areEqual());
    }

    /**
     * TestCase: Start with stream positions.
     */
    @Test
    public void testStartWithStreamPosition() throws Exception {
        Map<RangeId, EventPositionImpl> positions1 = prepareRanges(streamId1, 10);
        Map<RangeId, EventPositionImpl> positions2 = prepareRanges(streamId2, 5);

        StreamPositionImpl streamPos1 = new StreamPositionImpl(streamId1);
        StreamPositionImpl streamPos2 = new StreamPositionImpl(streamId2);
        positions1.values().forEach(streamPos1::addPosition);
        positions2.values().forEach(streamPos2::addPosition);

        Map<String, Position> streamPositions = Maps.newHashMap();
        streamPositions.put(streamName1, streamPos1);
        streamPositions.put(streamName2, streamPos2);

        LocalReadGroupController lrg = createLocalReadGroup(streamPositions);
        FutureUtils.result(lrg.start(actionObserver));

        assertEquals(actionObserver, lrg.getActionObserver());
        assertEquals(LocalReadGroupController.State.INIT, lrg.getState());
        assertTrue(lrg.getAssignedRanges().isEmpty());
        assertTrue(lrg.getPendingRanges().isEmpty());
        Map<RangeId, EventPositionImpl> union = Maps.newHashMap();
        union.putAll(positions1);
        union.putAll(positions2);
        MapDifference<RangeId, EventPositionImpl> diff = Maps.difference(union, lrg.getUnassignedRanges());
        assertTrue(diff.areEqual());
    }

    /**
     * TestCase: All streams have position.
     */
    @Test
    public void testStartWithPositionTail() throws Exception {
        Map<RangeId, EventPositionImpl> union = Maps.newHashMap();

        int numContainers = 1024;
        StorageContainerPlacementPolicy placementPolicy =
            (streamId, rangeId) -> ThreadLocalRandom.current().nextInt(numContainers);

        List<RangeProperties> streamRanges1 = ProtoUtils.split(
            streamId1, 24, 1000L, placementPolicy
        );
        NavigableMap<Long, RangeProperties> hashRanges1 = new TreeMap<>();
        streamRanges1.forEach(range -> {
            hashRanges1.put(range.getStartHashKey(), range);
            union.put(
                RangeId.of(streamId1, range.getRangeId()),
                EventPositionImpl.of(
                    range.getRangeId(),
                    0L,
                    0L,
                    0
                ));
        });
        when(metaRangeClient1.getActiveDataRanges())
            .thenReturn(FutureUtils.value(HashStreamRanges.ofHash(RangeKeyType.HASH, hashRanges1)));

        List<RangeProperties> streamRanges2 = ProtoUtils.split(
            streamId1, 48, 2000L, placementPolicy
        );
        NavigableMap<Long, RangeProperties> hashRanges2 = new TreeMap<>();
        streamRanges2.forEach(range -> {
            hashRanges2.put(range.getStartHashKey(), range);
            union.put(
                RangeId.of(streamId2, range.getRangeId()),
                EventPositionImpl.of(
                    range.getRangeId(),
                    0L,
                    0L,
                    0
                ));
        });
        when(metaRangeClient2.getActiveDataRanges())
            .thenReturn(FutureUtils.value(HashStreamRanges.ofHash(RangeKeyType.HASH, hashRanges2)));

        Map<String, Position> streamPositions = Maps.newHashMap();
        streamPositions.put(streamName1, Position.TAIL);
        streamPositions.put(streamName2, Position.TAIL);
        LocalReadGroupController lrg = createLocalReadGroup(streamPositions);
        FutureUtils.result(lrg.start(actionObserver));

        assertEquals(actionObserver, lrg.getActionObserver());
        assertEquals(LocalReadGroupController.State.INIT, lrg.getState());
        assertTrue(lrg.getAssignedRanges().isEmpty());
        assertTrue(lrg.getPendingRanges().isEmpty());

        MapDifference<RangeId, EventPositionImpl> diff = Maps.difference(union, lrg.getUnassignedRanges());
        assertTrue(diff.areEqual());
    }

    private Pair<LocalReadGroupController, Map<RangeId, EventPositionImpl>> startLocalGroupWith2Streams()
            throws Exception {
        return startLocalGroupWith2Streams(true);
    }

    private Pair<LocalReadGroupController, Map<RangeId, EventPositionImpl>>
            startLocalGroupWith2Streams(boolean startGroup) throws Exception {
        StreamPositionImpl streamPos1 = prepareStreamPosition(streamId1, 10);
        StreamPositionImpl streamPos2 = prepareStreamPosition(streamId2, 5);
        Map<RangeId, EventPositionImpl> union = Maps.newHashMap();
        union.putAll(streamPos1.getRangePositions()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                e -> RangeId.of(streamId1, e.getKey()),
                e -> e.getValue()
            ))
        );
        union.putAll(streamPos2.getRangePositions()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                e -> RangeId.of(streamId2, e.getKey()),
                e -> e.getValue()
            ))
        );

        Map<String, Position> streamPositions = Maps.newHashMap();
        streamPositions.put(streamName1, streamPos1);
        streamPositions.put(streamName2, streamPos2);
        LocalReadGroupController lrg = createLocalReadGroup(streamPositions);
        if (startGroup) {
            FutureUtils.result(lrg.start(actionObserver));
        }
        return Pair.of(lrg, union);
    }

    private void reinitOnWrongRevision(LocalReadGroupController.State expectedAtState,
                                       long expectedAtRevision,
                                       long currentRevision,
                                       ReadGroupCommand.Op op) throws Exception {
        Pair<LocalReadGroupController, Map<RangeId, EventPositionImpl>> startPair =
            startLocalGroupWith2Streams();
        LocalReadGroupController lrg = startPair.getLeft();

        AtomicReference<ReadGroupAction> actionRef = new AtomicReference<>(null);
        doAnswer(invocationOnMock -> {
            ReadGroupAction action = invocationOnMock.getArgument(0);
            actionRef.set(action);
            return null;
        }).when(actionObserver).onNext(any(ReadGroupAction.class));

        // set the read group to expected state and revision
        lrg.setState(expectedAtState);
        lrg.setRevision(expectedAtRevision);

        ReadGroupCommand command = ReadGroupCommand.newBuilder()
            .setOp(op)
            .setReaderRevision(currentRevision)
            .build();
        lrg.unsafeProcessReadGroupCommand(command);

        assertEquals(LocalReadGroupController.State.CLOSE, lrg.getState());
        assertNotNull(actionRef.get());
        assertEquals(ReadGroupAction.Code.REINIT_REQUIRED, actionRef.get().getCode());
        assertEquals(expectedAtRevision, actionRef.get().getReaderRevision());
    }

    @Test
    public void testProcessReadGroupCommandWrongRevision() throws Exception {
        reinitOnWrongRevision(
            LocalReadGroupController.State.INIT, 10L, 8L, ReadGroupCommand.Op.ADD_READER);
        reinitOnWrongRevision(
            LocalReadGroupController.State.ADDED, 10L, 8L, ReadGroupCommand.Op.COMPLETE_RANGE);
        reinitOnWrongRevision(
            LocalReadGroupController.State.ADDED, 10L, 8L, ReadGroupCommand.Op.REM_READER);
        reinitOnWrongRevision(
            LocalReadGroupController.State.ADDED, 10L, 8L, ReadGroupCommand.Op.UPDATE_POS);
    }

    private void reinitOnWrongState(LocalReadGroupController.State currentState,
                                    long expectedAtRevision,
                                    ReadGroupCommand.Op op) throws Exception {
        Pair<LocalReadGroupController, Map<RangeId, EventPositionImpl>> startPair =
            startLocalGroupWith2Streams();
        LocalReadGroupController lrg = startPair.getLeft();

        AtomicReference<ReadGroupAction> actionRef = new AtomicReference<>(null);
        doAnswer(invocationOnMock -> {
            ReadGroupAction action = invocationOnMock.getArgument(0);
            actionRef.set(action);
            return null;
        }).when(actionObserver).onNext(any(ReadGroupAction.class));

        // set the read group to expected state and revision
        lrg.setState(currentState);
        lrg.setRevision(expectedAtRevision);

        ReadGroupCommand command = ReadGroupCommand.newBuilder()
            .setOp(op)
            .setReaderRevision(expectedAtRevision)
            .build();
        lrg.unsafeProcessReadGroupCommand(command);

        assertEquals(LocalReadGroupController.State.CLOSE, lrg.getState());
        assertNotNull(actionRef.get());
        assertEquals(ReadGroupAction.Code.REINIT_REQUIRED, actionRef.get().getCode());
        assertEquals(expectedAtRevision, actionRef.get().getReaderRevision());
    }

    @Test
    public void testProcessReadGroupCommandWrongState() throws Exception {
        reinitOnWrongState(
            LocalReadGroupController.State.UNINIT, 8L, ReadGroupCommand.Op.ADD_READER);
        reinitOnWrongState(
            LocalReadGroupController.State.INIT, 8L, ReadGroupCommand.Op.COMPLETE_RANGE);
        reinitOnWrongState(
            LocalReadGroupController.State.INIT, 8L, ReadGroupCommand.Op.REM_READER);
        reinitOnWrongState(
            LocalReadGroupController.State.INIT, 8L, ReadGroupCommand.Op.UPDATE_POS);
    }

    private void reinitOnWrongCommand(ReadGroupCommand.Op op) throws Exception {
        Pair<LocalReadGroupController, Map<RangeId, EventPositionImpl>> startPair =
            startLocalGroupWith2Streams();
        LocalReadGroupController lrg = startPair.getLeft();

        AtomicReference<ReadGroupAction> actionRef = new AtomicReference<>(null);
        doAnswer(invocationOnMock -> {
            ReadGroupAction action = invocationOnMock.getArgument(0);
            actionRef.set(action);
            return null;
        }).when(actionObserver).onNext(any(ReadGroupAction.class));

        ReadGroupCommand command = ReadGroupCommand.newBuilder()
            .setOp(op)
            .setReaderRevision(0L)
            .build();
        lrg.unsafeProcessReadGroupCommand(command);

        assertEquals(LocalReadGroupController.State.CLOSE, lrg.getState());
        assertNotNull(actionRef.get());
        assertEquals(ReadGroupAction.Code.REINIT_REQUIRED, actionRef.get().getCode());
        assertEquals(0L, actionRef.get().getReaderRevision());
    }

    @Test
    public void testProcessReadGroupCommandWrongCommand() throws Exception {
        reinitOnWrongCommand(ReadGroupCommand.Op.ACQUIRE_RANGE);
        reinitOnWrongCommand(ReadGroupCommand.Op.RELEASE_RANGE);
    }

    private ReadGroupCommand.Builder newCommandBuilder(String readerName,
                                                       long readerRevision,
                                                       ReadGroupCommand.Op op) {
        return ReadGroupCommand.newBuilder()
            .setReadGroupId(-1L)
            .setReaderName(readerName)
            .setLeaseTtlMs(0)
            .setReaderRevision(readerRevision)
            .setOp(op);
    }

    @Test
    public void testStateTransitions() throws Exception {
        Pair<LocalReadGroupController, Map<RangeId, EventPositionImpl>> startPair =
            startLocalGroupWith2Streams(false);
        LocalReadGroupController lrg = startPair.getLeft();
        Map<RangeId, EventPositionImpl> union = startPair.getRight();

        LinkedBlockingQueue<ReadGroupAction> actionQueue = new LinkedBlockingQueue<>();
        doAnswer(invocationOnMock -> {
            ReadGroupAction action = invocationOnMock.getArgument(0);
            log.info("Received action {}.", action);
            actionQueue.add(action);
            return null;
        }).when(actionObserver).onNext(any(ReadGroupAction.class));

        // in UNINIT state
        assertEquals(LocalReadGroupController.State.UNINIT, lrg.getState());
        assertNull(lrg.getActionObserver());
        FutureUtils.result(lrg.start(actionObserver));

        long revision = 0L;

        // in INIT state
        assertEquals(LocalReadGroupController.State.INIT, lrg.getState());
        assertNotNull(lrg.getActionObserver());
        assertEquals(revision, lrg.getRevision());

        String readerName = "test-reader";

        // add reader
        ReadGroupCommand command = newCommandBuilder(readerName, revision, ReadGroupCommand.Op.ADD_READER)
            .setAddCmd(AddReaderCommand.newBuilder())
            .build();
        lrg.onNext(command);
        ReadGroupAction action = actionQueue.take();
        assertEquals(ReadGroupAction.Code.READER_ADDED, action.getCode());
        action = actionQueue.take();
        assertEquals(LocalReadGroupController.State.ADDED, lrg.getState());
        assertEquals(++revision, lrg.getRevision());
        assertTrue(lrg.getUnassignedRanges().isEmpty());
        MapDifference<RangeId, EventPositionImpl> diff = Maps.difference(union, lrg.getAssignedRanges());
        assertTrue(diff.areEqual());
        assertEquals(ReadGroupAction.Code.ACQUIRE_RANGE, action.getCode());
        AcquireRangeAction acquireRangeAction = action.getAcquireAction();
        assertEquals(PositionUtils.fromPositionImpls(union), acquireRangeAction.getReadPos());

        // update position
        command = newCommandBuilder(readerName, revision, ReadGroupCommand.Op.UPDATE_POS)
            .setUpdateCmd(UpdatePositionCommand.newBuilder()
                .setReadPos(PositionUtils.fromPositionImpls(union)))
            .build();
        lrg.onNext(command);
        action = actionQueue.take();
        assertEquals(LocalReadGroupController.State.ADDED, lrg.getState());
        assertEquals(revision, lrg.getRevision());
        assertTrue(lrg.getUnassignedRanges().isEmpty());
        diff = Maps.difference(union, lrg.getAssignedRanges());
        assertTrue(diff.areEqual());
        assertEquals(ReadGroupAction.Code.POSTION_UPDATED, action.getCode());

        // complete range
        Set<Long> parents = Sets.newHashSet(1L, 2L, 3L, 4L, 5L);
        for (long rid = 1L; rid <= 5L; rid++) {
            command = newCommandBuilder(readerName, revision, ReadGroupCommand.Op.COMPLETE_RANGE)
                .setCompleteCmd(CompleteRangeCommand.newBuilder()
                    .setStreamId(streamId1)
                    .setRangeId(rid)
                    .setChildRanges(ParentRangesList.newBuilder()
                        .addChildRanges(ParentRanges.newBuilder()
                            .setRangeId(11L)
                            .addAllParentRangeIds(Lists.newArrayList(1L, 2L, 3L, 4L, 5L))
                        )))
                .build();
            lrg.onNext(command);
            action = actionQueue.take();

            assertEquals(LocalReadGroupController.State.ADDED, lrg.getState());
            assertEquals(++revision, lrg.getRevision());

            // verify pendings
            parents.remove(rid);

            // verify unassigned
            assertTrue(lrg.getUnassignedRanges().isEmpty());
            union.remove(RangeId.of(streamId1, rid));

            // verify assigned
            if (rid == 5L) {
                union.put(
                    RangeId.of(streamId1, 11L),
                    EventPositionImpl.createInitPos(11L));
            }
            diff = Maps.difference(union, lrg.getAssignedRanges());
            assertTrue(diff.areEqual());

            if (rid < 5L) {
                assertFalse(lrg.getPendingRanges().isEmpty());
                assertEquals(1, lrg.getPendingRanges().size());
                assertEquals(parents, lrg.getPendingRanges().get(RangeId.of(streamId1, 11L)));
                assertEquals(ReadGroupAction.Code.POSTION_UPDATED, action.getCode());
            } else {
                assertTrue(lrg.getPendingRanges().isEmpty());
                assertEquals(ReadGroupAction.Code.ACQUIRE_RANGE, action.getCode());
                Map<RangeId, EventPositionImpl> newRangeMap = Maps.newHashMap();
                newRangeMap.put(
                    RangeId.of(streamId1, 11L),
                    EventPositionImpl.createInitPos(11L));
                assertEquals(
                    PositionUtils.fromPositionImpls(newRangeMap),
                    action.getAcquireAction().getReadPos());
            }
            assertEquals(revision, action.getReaderRevision());
        }


        // remove reader
        command = newCommandBuilder(readerName, revision, ReadGroupCommand.Op.REM_READER)
            .setRemoveCmd(RemoveReaderCommand.newBuilder()
                .setReadPos(PositionUtils.fromPositionImpls(union)))
            .build();
        lrg.onNext(command);
        action = actionQueue.take();
        assertEquals(LocalReadGroupController.State.REMOVED, lrg.getState());
        assertEquals(++revision, lrg.getRevision());
        assertTrue(lrg.getUnassignedRanges().isEmpty());
        diff = Maps.difference(union, lrg.getAssignedRanges());
        assertTrue(diff.areEqual());
        assertEquals(ReadGroupAction.Code.READER_REMOVED, action.getCode());
    }

}
