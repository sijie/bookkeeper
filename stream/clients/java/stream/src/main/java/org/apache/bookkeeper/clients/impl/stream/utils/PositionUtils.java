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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.bookkeeper.clients.impl.stream.event.EventPositionImpl;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.ReadPosition;
import org.apache.bookkeeper.stream.proto.StreamPosition;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * Utils to handle positions.
 */
public final class PositionUtils {

    private PositionUtils() {}

    //
    // Read Positions
    //

    /**
     * Build a {@link ReadPosition} from a map of range positions.
     *
     * @param ranges a map of range positions.
     * @return the built {@link ReadPosition}
     */
    public static ReadPosition fromRangePositions(Map<RangeId, RangePosition> ranges) {
        ReadPosition.Builder posBuilder = ReadPosition.newBuilder();
        Map<Long, StreamPosition.Builder> streamPosBuilders = Maps.newHashMap();
        for (Map.Entry<RangeId, RangePosition> posEntry : ranges.entrySet()) {
            RangeId rangeId = posEntry.getKey();
            StreamPosition.Builder streamPosBuilder = streamPosBuilders.getOrDefault(
                rangeId.getStreamId(),
                StreamPosition.newBuilder().setStreamId(rangeId.getStreamId()));
            RangePosition rangePos = posEntry.getValue();
            streamPosBuilder.addRangePositions(rangePos);
            streamPosBuilders.put(rangeId.getStreamId(), streamPosBuilder);
        }
        streamPosBuilders.values().forEach(posBuilder::addStreamPositions);
        return posBuilder.build();
    }

    /**
     * Build a {@link ReadPosition} from a map of range positions.
     *
     * @param ranges a map of range positions.
     * @return the built {@link ReadPosition}
     */
    public static ReadPosition fromPositionImpls(Map<RangeId, EventPositionImpl> ranges) {
        ReadPosition.Builder posBuilder = ReadPosition.newBuilder();
        Map<Long, StreamPosition.Builder> streamPosBuilders = Maps.newHashMap();
        for (Map.Entry<RangeId, EventPositionImpl> posEntry : ranges.entrySet()) {
            RangeId rangeId = posEntry.getKey();
            EventPositionImpl posImpl = posEntry.getValue();
            StreamPosition.Builder streamPosBuilder = streamPosBuilders.getOrDefault(
                rangeId.getStreamId(),
                StreamPosition.newBuilder().setStreamId(rangeId.getStreamId()));
            streamPosBuilder.addRangePositions(RangePosition.newBuilder()
                .setRangeId(rangeId.getRangeId())
                .setOffset(posImpl.getRangeOffset())
                .setSeqNum(posImpl.getRangeOffset())
                .setSlotId(posImpl.getSlotId()));
            streamPosBuilders.put(rangeId.getStreamId(), streamPosBuilder);
        }
        streamPosBuilders.values().forEach(posBuilder::addStreamPositions);
        return posBuilder.build();
    }

    public static Map<RangeId, RangePosition> toRangePositionMap(ReadPosition readPos) {
        Map<RangeId, RangePosition> rangePosMap = Maps.newHashMap();
        for (StreamPosition streamPos : readPos.getStreamPositionsList()) {
            long streamId = streamPos.getStreamId();
            for (RangePosition rangePos : streamPos.getRangePositionsList()) {
                long rangeId = rangePos.getRangeId();
                RangeId range = RangeId.of(streamId, rangeId);
                RangePosition eventPos = rangePos;
                rangePosMap.put(range, eventPos);
            }
        }
        return rangePosMap;
    }

    public static Map<RangeId, EventPositionImpl> toPositionImpls(ReadPosition readPos) {
        Map<RangeId, EventPositionImpl> rangePosMap = Maps.newHashMap();
        for (StreamPosition streamPos : readPos.getStreamPositionsList()) {
            long streamId = streamPos.getStreamId();
            for (RangePosition rangePos : streamPos.getRangePositionsList()) {
                long rangeId = rangePos.getRangeId();
                RangeId range = RangeId.of(streamId, rangeId);
                rangePosMap.put(range, EventPositionImpl.of(
                    rangePos.getRangeId(),
                    rangePos.getOffset(),
                    rangePos.getSeqNum(),
                    rangePos.getSlotId()
                ));
            }
        }
        return rangePosMap;
    }

    public static Map<RangeId, EventPositionImpl> toPositionImpls(StreamPosition streamPos) {
        Map<RangeId, EventPositionImpl> rangePosMap = Maps.newHashMap();
        long streamId = streamPos.getStreamId();
        for (RangePosition rangePos : streamPos.getRangePositionsList()) {
            long rangeId = rangePos.getRangeId();
            RangeId range = RangeId.of(streamId, rangeId);
            rangePosMap.put(range, EventPositionImpl.of(
                rangePos.getRangeId(),
                rangePos.getOffset(),
                rangePos.getSeqNum(),
                rangePos.getSlotId()
            ));
        }
        return rangePosMap;
    }

}
