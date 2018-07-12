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

package org.apache.bookkeeper.stream.storage.api.stream;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetRequest;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetResponse;

/**
 * A writer that appends events into a stream range.
 */
public interface StreamRangeWriter {

    /**
     * Get the writer id.
     *
     * @return writer id.
     */
    long getWriterId();

    /**
     * Get the last event set id received from the write session.
     *
     * @return the last event set id received from the write session.
     */
    long getLastEventSetId();

    /**
     * Append an event set to the stream range.
     *
     * @param request the event set request.
     * @return the write response.
     */
    CompletableFuture<WriteEventSetResponse> write(WriteEventSetRequest request);

    /**
     * Called when a write session is aborted (e.g. exceptions received on this session).
     */
    void onWriteSessionAborted(Throwable cause);

    /**
     * Called when a write session is completed. (e.g. client disconnected)
     */
    void onWriteSessionCompleted();

}
