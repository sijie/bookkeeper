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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.clients.impl.stream.utils.PositionUtils;
import org.apache.bookkeeper.stream.proto.ReadPosition;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * An instance represents the position in a stream.
 *
 * <p>It contains a list of range positions.
 */
@EqualsAndHashCode
@ToString
public class RangesPositionImpl implements Position {

    private static final long serialVersionUID = -1138660620550414576L;

    private final Map<RangeId, EventPositionImpl> rangePositions;

    public RangesPositionImpl() {
        this.rangePositions = new HashMap<>();
    }

    public RangesPositionImpl(Map<RangeId, EventPositionImpl> positions) {
        this.rangePositions = Maps.newHashMap(positions);
    }

    public synchronized EventPositionImpl getPosition(RangeId rangeId) {
        return rangePositions.get(rangeId);
    }

    public synchronized RangesPositionImpl updatePosition(
        RangeId rangeId, EventPositionImpl eventPos) {
        this.rangePositions.put(rangeId, eventPos);
        return this;
    }

    @VisibleForTesting
    Map<RangeId, EventPositionImpl> getRangePositions() {
        return rangePositions;
    }

    public synchronized RangesPositionImpl snapshot() {
        return new RangesPositionImpl(rangePositions);
    }

    public synchronized ReadPosition toReadPosition() {
        return PositionUtils.fromPositionImpls(rangePositions);
    }

}
