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

import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.api.stream.Position;

/**
 * An instance represents the position in a stream.
 *
 * <p>It contains a list of range positions.
 */
public class StreamPositionImpl implements Position {

    private final long streamId;
    private final Map<Long, Position> rangePositions;

    public StreamPositionImpl(long streamId) {
        this.streamId = streamId;
        this.rangePositions = new HashMap<>();
    }

    public long getStreamId() {
        return streamId;
    }

    public StreamPositionImpl addPosition(EventPositionImpl rangePos) {
        this.rangePositions.put(rangePos.getRangeId(), rangePos);
        return this;
    }

    public Map<Long, Position> getRangePositions() {
        return rangePositions;
    }

}
