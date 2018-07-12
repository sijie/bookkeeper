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

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.api.stream.exceptions.StreamApiException;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * Reader to read events from a stream in order.
 *
 * @param <KeyT> the key type of the events.
 * @param <ValueT> the value type of the events.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Reader<KeyT, ValueT> extends AutoCloseable {

    /**
     * Read the next events in the stream.
     *
     * <p>The future returned by this call will be satisfied when there are events available for reading.
     * If there are no events available at the time this method is called, the client will wait until first
     * event is available. The {@code waitTime} will be ignored at this case. If there are events available
     * at the time this method is called, the client will wait up to the specified {@code waitTime} before
     * satisfying the callback.
     *
     * @param waitTime maximum wait time when there are events to read.
     * @param timeUnit time unit of the wait time.
     * @return the read events. it contains an iterator to iterate over the events.
     */
    ReadEvents<KeyT, ValueT> readNext(long waitTime, TimeUnit timeUnit)
        throws StreamApiException;

    /**
     * Return the current reader position.
     *
     * @return the current reader position.
     */
    Position getPosition();

}
