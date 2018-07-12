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

package org.apache.bookkeeper.clients.impl.stream.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.bookkeeper.clients.impl.stream.utils.PositionUtils;
import org.apache.bookkeeper.stream.proto.ReadPosition;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.junit.Test;

/**
 * Unit test {@link RangesPositionImpl}.
 */
public class RangesPositionImplTest {

    @Test
    public void testUpdate() {
        RangesPositionImpl rangePos = new RangesPositionImpl();

        RangeId rid = RangeId.of(1234L, 4567L);
        assertNull(rangePos.getPosition(rid));
        // update position
        EventPositionImpl eventPos = EventPositionImpl.of(
            1234L,
            3456L,
            7890L,
            0);
        rangePos.updatePosition(rid, eventPos);
        EventPositionImpl returnedEventPos = rangePos.getPosition(rid);
        assertSame(eventPos, returnedEventPos);
    }

    @Test
    public void testSnapshot() {
        RangesPositionImpl rangePos = new RangesPositionImpl();
        Map<RangeId, EventPositionImpl> expectedRangePositions = Maps.newHashMap();
        int numRanges = 20;
        long streamId = System.currentTimeMillis();
        for (int i = 0; i < numRanges; i++) {
            RangeId rid = RangeId.of(streamId, i);
            EventPositionImpl eventPos = EventPositionImpl.of(
                i,
                i * 100,
                i * 1000,
                i);
            rangePos.updatePosition(rid, eventPos);
            // put the range into expected map for comparison
            expectedRangePositions.put(rid, eventPos);
        }
        // take a snapshot
        RangesPositionImpl snapshotPos = rangePos.snapshot();
        assertNotSame(rangePos, snapshotPos);

        Map<RangeId, EventPositionImpl> snapshottedRangePositions = snapshotPos.getRangePositions();
        MapDifference<RangeId, EventPositionImpl> diff =
            Maps.difference(expectedRangePositions, snapshottedRangePositions);
        assertTrue(diff.areEqual());

        // mutate the original range pos
        for (int i = 0; i < numRanges; i++) {
            RangeId rid = RangeId.of(streamId, i);
            EventPositionImpl eventPos = EventPositionImpl.of(
                i + 1,
                (i + 1) * 200,
                (i + 1) * 2000,
                i + 1);
            rangePos.updatePosition(rid, eventPos);
        }

        // compare the ranges
        for (int i = 0; i < numRanges; i++) {
            RangeId rid = RangeId.of(streamId, i);
            EventPositionImpl originalEventPos = rangePos.getPosition(rid);
            EventPositionImpl snapshottedEventPos = snapshotPos.getPosition(rid);
            assertNotNull(originalEventPos);
            assertNotNull(snapshottedEventPos);
            assertNotEquals(originalEventPos, snapshottedEventPos);
        }
    }

    @Test
    public void testToReadPosition() {
        RangesPositionImpl rangePos = new RangesPositionImpl();
        int numRanges = 20;
        long streamId = System.currentTimeMillis();
        for (int i = 0; i < numRanges; i++) {
            RangeId rid = RangeId.of(streamId, i);
            EventPositionImpl eventPos = EventPositionImpl.of(
                i,
                i * 100,
                i * 1000,
                i);
            rangePos.updatePosition(rid, eventPos);
        }

        ReadPosition readPosition = rangePos.toReadPosition();
        Map<RangeId, EventPositionImpl> rangePositions =
            PositionUtils.toPositionImpls(readPosition);
        RangesPositionImpl parsedRangePos = new RangesPositionImpl(rangePositions);

        assertEquals(rangePos, parsedRangePos);
    }

}
