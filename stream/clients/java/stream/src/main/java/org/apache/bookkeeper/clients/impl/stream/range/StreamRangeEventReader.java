/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.impl.stream.range;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.stream.ReadEvents;
import org.apache.bookkeeper.common.util.AutoAsyncCloseable;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * A reader that reads events from a provided data range.
 */
public interface StreamRangeEventReader<KeyT, ValueT> extends AutoAsyncCloseable {

    /**
     * Get the range id.
     *
     * @return the range id.
     */
    RangeId getRangeId();

    /**
     * Initialize the reader.
     *
     * @return a future represents the result of initialization.
     */
    CompletableFuture<StreamRangeEventReader<KeyT, ValueT>> initialize();

    /**
     * Read next batch of events.
     *
     * @return a future represents the read result.
     */
    CompletableFuture<ReadEvents<KeyT, ValueT>> readNext();

}
