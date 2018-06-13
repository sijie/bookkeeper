/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.api.stream;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.common.util.AutoAsyncCloseable;

/**
 * A writer writes events to a stream.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Writer<KeyT, ValueT> extends AutoAsyncCloseable {

    /**
     * Return the event builder.
     *
     * @return event builder to build events to append to the stream.
     */
    WriteEventBuilder<KeyT, ValueT> eventBuilder();

    /**
     * Initialize the writer.
     *
     * @return the initialized writer.
     */
    CompletableFuture<Writer<KeyT, ValueT>> initialize();

    /**
     * Write an {@code event} to the stream.
     *
     * <p>An event can be associated with a <tt>key</tt> and an event <tt>timestamp</tt>.
     * Events written here should only be written exactly once to the stream despite of connection failures.
     * The events will appear in the same order that they were written.
     *
     * <p>Application can provided a <tt>future</tt> to listen on the write result. The <tt>future</tt>s are
     * satisfied in the order of events are written and in the same thread. Application can rely on this ordering
     * guarantee.
     *
     * <p>The implementation handles any failures happened in stream and will not volatile exactly once semantic.
     * External retries will potentially volatile exactly once semantic.
     *
     * @param event event to append to the stream.
     * @return future represents the write result
     */
    CompletableFuture<WriteResult> write(WriteEvent<KeyT, ValueT> event);

    /**
     * Create a {@link WriteBatch} to append events to the stream as an atomic batch.
     *
     * @return new write batch
     */
    WriteBatch<KeyT, ValueT> newBatch();

    /**
     * Issue flush request to flush all the events that have been passed to the writer by
     * {@link #write(WriteEvent)}.
     *
     * <p>The future returned by this call will be satisfied when all the events have been
     * successfully written and their futures are all completed.
     *
     * @return the future indicating the flush result.
     */
    CompletableFuture<FlushResult> flush();

}
