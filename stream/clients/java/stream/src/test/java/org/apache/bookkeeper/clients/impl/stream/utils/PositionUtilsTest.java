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

package org.apache.bookkeeper.clients.impl.stream.utils;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bookkeeper.clients.impl.stream.event.EventPositionImpl;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.ReadPosition;
import org.apache.bookkeeper.stream.proto.StreamPosition;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.junit.Test;

/**
 * Unit test {@link PositionUtils}.
 */
public class PositionUtilsTest {

    @Test
    public void testFromToRangePositions() {
        Map<RangeId, RangePosition> ranges = Maps.newHashMap();
        int numRanges = 20;
        for (int i = 0; i < numRanges; i++) {
            long streamId = ThreadLocalRandom.current().nextLong(1024L);
            long rangeId = ThreadLocalRandom.current().nextLong(1024L);
            RangeId rid = RangeId.of(streamId, rangeId);
            RangePosition rangePos = RangePosition.newBuilder()
                .setRangeId(rangeId)
                .setOffset((i + 1) * 100)
                .setSeqNum((i + 1) * 1000)
                .setSlotId(i + 1)
                .build();
            ranges.put(rid, rangePos);
        }

        ReadPosition readPos = PositionUtils.fromRangePositions(ranges);

        Map<RangeId, RangePosition> readRanges = PositionUtils.toRangePositionMap(readPos);
        assertEquals(ranges, readRanges);
    }

    @Test
    public void testFromToEventPositionImpls() {
        Map<RangeId, EventPositionImpl> ranges = Maps.newHashMap();
        int numRanges = 20;
        for (int i = 0; i < numRanges; i++) {
            long streamId = ThreadLocalRandom.current().nextLong(1024L);
            long rangeId = ThreadLocalRandom.current().nextLong(1024L);
            RangeId rid = RangeId.of(streamId, rangeId);
            EventPositionImpl eventPos = EventPositionImpl.of(
                rangeId,
                (i + 1) * 100,
                (i + 1) * 1000,
                i + 1);
            ranges.put(rid, eventPos);
        }

        ReadPosition readPos = PositionUtils.fromPositionImpls(ranges);

        Map<RangeId, EventPositionImpl> readRanges = PositionUtils.toPositionImpls(readPos);
        assertEquals(ranges, readRanges);
    }

    @Test
    public void testFromStreamPositionToEventPositionImpls() {
        Map<RangeId, EventPositionImpl> ranges = Maps.newHashMap();
        StreamPosition.Builder streamPosBuilder = StreamPosition.newBuilder();
        int numRanges = 20;
        long streamid = ThreadLocalRandom.current().nextLong(1024L);
        for (int i = 0; i < numRanges; i++) {
            long rangeId = ThreadLocalRandom.current().nextLong(1024L);
            RangeId rid = RangeId.of(streamid, rangeId);
            EventPositionImpl eventPos = EventPositionImpl.of(
                rangeId,
                (i + 1) * 100,
                (i + 1) * 1000,
                i + 1);
            ranges.put(rid, eventPos);

            RangePosition rangePos = RangePosition.newBuilder()
                .setRangeId(rangeId)
                .setOffset(eventPos.getRangeOffset())
                .setSeqNum(eventPos.getRangeSeqNum())
                .setSlotId(eventPos.getSlotId())
                .build();
            streamPosBuilder = streamPosBuilder.addRangePositions(rangePos);
        }
        StreamPosition streamPos = streamPosBuilder
            .setStreamId(streamid)
            .build();

        Map<RangeId, EventPositionImpl> readRanges = PositionUtils.toPositionImpls(streamPos);
        assertEquals(ranges, readRanges);
    }

}
