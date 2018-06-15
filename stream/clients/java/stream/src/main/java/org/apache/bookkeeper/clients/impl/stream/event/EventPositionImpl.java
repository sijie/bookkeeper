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

import lombok.Data;
import org.apache.bookkeeper.api.stream.Position;

/**
 * Default implementation of {@link Position}.
 */
@Data(staticConstructor = "of")
public class EventPositionImpl implements Position {

    private static final long serialVersionUID = -7670268899279045922L;

    private final long rangeId;
    private final long rangeOffset;
    private final long rangeSeqNum;
    private final int slotId;

    public static EventPositionImpl createInitPos(long rangeId) {
        return of(rangeId, 0L, 0L, 0);
    }

}
