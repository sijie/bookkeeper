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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.protobuf.UnsafeByteOperations;
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetRequest;

/**
 * A pending write to the range server.
 */
@Data(staticConstructor = "of")
public class PendingWrite {

    public static PendingWrite of(PendingWrite write,
                                  CompletableFuture<RangePositionImpl> writeFuture) {
        PendingEventSet eventSet = write.getEventSet();
        eventSet.retain();
        return of(
            write.getSequenceId(),
            eventSet,
            writeFuture);
    }

    /**
     * The client side sequence id that is used for de-duplication.
     */
    private final long sequenceId;
    /**
     * the data to be written.
     */
    private final PendingEventSet eventSet;
    /**
     * The future to be satisfied when the data is written.
     */
    private final CompletableFuture<RangePositionImpl> writeFuture;

    private boolean done = false;
    private RangePositionImpl rangePosition = null;
    private Throwable cause = null;

    public synchronized WriteEventSetRequest toWriteEventSetRequest() {
        return WriteEventSetRequest.newBuilder()
            .setEventSetId(getSequenceId())
            .setData(UnsafeByteOperations.unsafeWrap(eventSet.getEventSetBuf().nioBuffer()))
            .build();
    }

    public synchronized RangePositionImpl getRangePosition() {
        return rangePosition;
    }

    public synchronized Throwable getCause() {
        return cause;
    }

    public synchronized boolean isSuccess() {
        return done && null == cause;
    }

    public synchronized boolean isCompletedExceptionally() {
        return done && null != cause;
    }

    public synchronized void setRangePosition(RangePositionImpl pos) {
        done = true;
        this.rangePosition = pos;
        this.cause = null;
    }

    public synchronized void complete(RangePositionImpl pos) {
        setRangePosition(pos);
        submitCallback();
    }

    public synchronized void setCause(Throwable cause) {
        done = true;
        this.rangePosition = null;
        this.cause = cause;
    }

    public synchronized void completeExceptionally(Throwable cause) {
        setCause(cause);
        submitCallback();
    }

    public synchronized void submitCallback() {
        checkArgument(done, "submit callback before completing it");
        if (null != cause) {
            writeFuture.completeExceptionally(cause);
        } else {
            checkNotNull(rangePosition,
                "invalid range position when submitting callback");
            writeFuture.complete(rangePosition);
        }
    }

    public synchronized void setDone(boolean done) {
        this.done = done;
    }

    public synchronized boolean getDone() {
        return done;
    }

    public synchronized boolean isDone() {
        return done;
    }

}
