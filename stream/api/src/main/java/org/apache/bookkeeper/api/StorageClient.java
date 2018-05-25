/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.api;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.stream.Stream;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.common.util.AutoAsyncCloseable;

/**
 * The stream storage client.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface StorageClient extends AutoAsyncCloseable {

    /**
     * Open a {@link PTable} to update provided <tt>table</tt>.
     *
     * @param table table name
     * @return a future represents the open result.
     */
    CompletableFuture<PTable<ByteBuf, ByteBuf>> openPTable(String table);

    /**
     * Open a {@link Table} to update the provided <tt>table</tt>.
     *
     * @param table table name
     * @return a future represents the open result.
     */
    CompletableFuture<Table<ByteBuf, ByteBuf>> openTable(String table);

    /**
     * Open a {@link Stream} to interact with provided <tt>stream</tt>.
     *
     * @param stream stream name.
     * @param config stream config.
     * @return a future to return the stream handle.
     */
    <KeyT, ValueT>
    CompletableFuture<Stream<KeyT, ValueT>> openStream(String stream,
                                                       StreamConfig<KeyT, ValueT> config);

}
