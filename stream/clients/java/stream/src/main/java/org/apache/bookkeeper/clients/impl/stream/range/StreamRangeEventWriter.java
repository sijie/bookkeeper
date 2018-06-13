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

package org.apache.bookkeeper.clients.impl.stream.range;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.WriteEvent;
import org.apache.bookkeeper.clients.impl.stream.event.PendingWrite;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.common.util.AutoAsyncCloseable;

/**
 * A writer that writes events to a provided data range.
 */
public interface StreamRangeEventWriter<KeyT, ValueT> extends AutoAsyncCloseable {

    /**
     * Write a pending event to a provided data range.
     *
     * @param event     the event to append to the data range.
     * @param future    the future to listen on the write result.
     */
    void write(WriteEvent<KeyT, ValueT> event,
               @Nullable CompletableFuture<Position> future)
        throws InternalStreamException;

    /**
     * Flush the current data range.
     *
     * @return flush the current data range.
     */
    CompletableFuture<Void> flush();

    /**
     * Retrieve the list of {@link PendingWrite}s that have been sent to the servers but have not yet been acknowledged.
     *
     * @return list of pending writes.
     */
    List<PendingWrite> closeAndGetPendingWrites();

}
