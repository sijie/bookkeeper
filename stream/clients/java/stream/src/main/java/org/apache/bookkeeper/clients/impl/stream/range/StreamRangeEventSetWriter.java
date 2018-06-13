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

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.clients.impl.stream.event.PendingWrite;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.common.util.AutoAsyncCloseable;

/**
 * A client that talks to range server to write chunk of data.
 */
public interface StreamRangeEventSetWriter extends AutoAsyncCloseable {

    /**
     * Initialize the event set writer.
     *
     * @return a future represents the result of initialization.
     */
    CompletableFuture<StreamRangeEventSetWriter> initialize();

    /**
     * Write chunk of data to range server.
     *
     * @param pendingWrite chunk of data to a range server.
     */
    void write(PendingWrite pendingWrite) throws InternalStreamException;

}
