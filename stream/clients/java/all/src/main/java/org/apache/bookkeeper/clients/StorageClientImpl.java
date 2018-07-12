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

package org.apache.bookkeeper.clients;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.Code;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.stream.Stream;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.exceptions.StreamApiException;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.impl.internal.StorageServerClientManagerImpl;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.impl.kv.ByteBufTableImpl;
import org.apache.bookkeeper.clients.impl.kv.PByteBufTableImpl;
import org.apache.bookkeeper.clients.impl.stream.StreamImpl;
import org.apache.bookkeeper.clients.utils.ClientResources;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.common.util.ExceptionUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.stream.proto.StorageType;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * The implementation of {@link StorageClient} client.
 */
@Slf4j
class StorageClientImpl extends AbstractAutoAsyncCloseable implements StorageClient {

    private static final String COMPONENT_NAME = StorageClientImpl.class.getSimpleName();

    private final String namespaceName;
    private final StorageClientSettings settings;
    private final ClientResources resources;
    private final OrderedScheduler scheduler;

    // clients
    private final StorageServerClientManager serverManager;

    public StorageClientImpl(String namespaceName,
                             StorageClientSettings settings,
                             ClientResources resources) {
        this.namespaceName = namespaceName;
        this.settings = settings;
        this.resources = resources;
        this.serverManager = new StorageServerClientManagerImpl(settings, resources.scheduler());
        this.scheduler = SharedResourceManager.shared().get(resources.scheduler());

    }

    CompletableFuture<StreamProperties> getStreamProperties(String streamName) {
        return this.serverManager.getRootRangeClient().getStream(namespaceName, streamName);
    }

    //
    // Streams
    //

    @Override
    public <KeyT, ValueT> CompletableFuture<Stream<KeyT, ValueT>>
            openStream(String stream, StreamConfig<KeyT, ValueT> config) {
        return getStreamProperties(stream).thenCompose(props -> {
            if (log.isInfoEnabled()) {
                log.info("Retrieved stream properties for stream {} : {}", stream, props);
            }
            if (StorageType.STREAM != props.getStreamConf().getStorageType()) {
                return FutureUtils.exception(
                    new StreamApiException(Code.ILLEGAL_OP,
                        "Can't open a non-stream storage entity : " + props.getStreamConf().getStorageType()));
            } else {
                return FutureUtils.value(new StreamImpl<>(
                    namespaceName,
                    props,
                    settings,
                    config,
                    serverManager,
                    scheduler
                ));
            }
        });
    }


    //
    // Tables
    //

    @Override
    public CompletableFuture<PTable<ByteBuf, ByteBuf>> openPTable(String streamName) {
        return ExceptionUtils.callAndHandleClosedAsync(
            COMPONENT_NAME,
            isClosed(),
            (future) -> openStreamAsTableImpl(streamName, future));
    }

    @Override
    public CompletableFuture<Table<ByteBuf, ByteBuf>> openTable(String table) {
        return openPTable(table)
            .thenApply(pTable -> new ByteBufTableImpl(pTable));
    }

    private void openStreamAsTableImpl(String streamName,
                                       CompletableFuture<PTable<ByteBuf, ByteBuf>> future) {
        FutureUtils.proxyTo(
            getStreamProperties(streamName).thenComposeAsync(props -> {
                if (log.isInfoEnabled()) {
                    log.info("Retrieved table properties for table {} : {}", streamName, props);
                }
                if (StorageType.TABLE != props.getStreamConf().getStorageType()) {
                    return FutureUtils.exception(new StreamApiException(
                        Code.ILLEGAL_OP,
                        "Can't open a non-table storage entity : " + props.getStreamConf().getStorageType())
                    );
                }
                return new PByteBufTableImpl(
                    streamName,
                    props,
                    serverManager,
                    scheduler.chooseThread(props.getStreamId()),
                    settings.backoffPolicy()
                ).initialize();
            }),
            future
        );
    }

    //
    // Closeable API
    //

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        scheduler.submit(() -> {
            serverManager.close();
            closeFuture.complete(null);
            SharedResourceManager.shared().release(resources.scheduler(), scheduler);
        });
    }

    @Override
    public void close() {
        try {
            FutureUtils.result(closeAsync(), 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            // ignore the exception
        }
        scheduler.forceShutdown(100, TimeUnit.MILLISECONDS);
    }
}
