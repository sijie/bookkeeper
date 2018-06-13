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

package org.apache.bookkeeper.clients.impl.stream.event;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.common.util.ReferenceCounted;

/**
 * A pending event set.
 */
@Data(staticConstructor = "of")
public class PendingEventSet implements ReferenceCounted {

    private final ByteBuf eventSetBuf;
    private final List<CompletableFuture<Position>> callbacks;

    /**
     * Completing the eventset will automatically {@link #release()} the references.
     *
     * @param eventSetPos sequence in the range
     */
    public void complete(RangePositionImpl eventSetPos) {
        int slotId = 0;
        for (CompletableFuture<Position> callback : callbacks) {
            if (null == callback) {
                ++slotId;
                continue;
            }
            callback.complete(EventPositionImpl.of(
                eventSetPos.getRangeId(),
                eventSetPos.getRangeOffset(),
                eventSetPos.getRangeSeqNum(),
                slotId
            ));
            ++slotId;
        }
        eventSetPos.release();
        release();
    }

    /**
     * Completing the eventset will automatically {@link #release()} the references.
     *
     * @param cause cause to complete the eventset exceptionally.
     */
    public void completeExceptionally(Throwable cause) {
        for (CompletableFuture<Position> callback : callbacks) {
            if (null != callback) {
                callback.completeExceptionally(cause);
            }
        }
        release();
    }

    @Override
    public void retain() {
        eventSetBuf.retain();
    }

    @Override
    public void release() {
        eventSetBuf.release();
    }
}
