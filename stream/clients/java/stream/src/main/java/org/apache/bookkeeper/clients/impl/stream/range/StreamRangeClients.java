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
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannelManager;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * A class that manages stream range writers and readers.
 */
@Slf4j
public class StreamRangeClients {

    // TODO: make it configurable
    private static final int MAX_READAHEAD_BYTES = 16 * 1024 * 1024;
    private static final int RECONNECT_BACKOFF_MS = 2000;

    private final OrderedScheduler scheduler;
    private final StorageContainerChannelManager scChannelManager;

    public StreamRangeClients(StorageContainerChannelManager scChannelManager,
                              OrderedScheduler scheduler) {
        this.scheduler = scheduler;
        this.scChannelManager = scChannelManager;
    }

    StorageContainerChannelManager getChannelManager() {
        return scChannelManager;
    }

    public CompletableFuture<StreamRangeEventSetWriter> openStreamRangeWriter(long streamId,
                                                                              RangeProperties rangeProps,
                                                                              String writerName) {
        CompletableFuture<StreamRangeEventSetWriter> openFuture = FutureUtils.createFuture();
        StorageContainerChannel scClient = this.scChannelManager.getOrCreate(rangeProps.getStorageContainerId());
        StreamRangeEventSetWriter writer = new StreamRangeEventSetWriterImpl(
            RangeId.of(streamId, rangeProps.getRangeId()),
            writerName,
            scheduler.chooseThread(streamId),
            scClient);
        writer.initialize().whenComplete((value, cause) -> {
            if (null == cause) {
                openFuture.complete(writer);
            } else {
                openFuture.completeExceptionally(cause);
            }
        });
        return openFuture;

    }

    public CompletableFuture<StreamRangeEventSetReader> openStreamRangeReader(long streamId,
                                                                              RangeProperties rangeProps,
                                                                              RangePosition pos) {
        CompletableFuture<StreamRangeEventSetReader> openFuture = FutureUtils.createFuture();
        StorageContainerChannel scClient = this.scChannelManager.getOrCreate(rangeProps.getStorageContainerId());
        RangeId rid = RangeId.of(streamId, rangeProps.getRangeId());
        StreamRangeEventSetReaderImpl reader = new StreamRangeEventSetReaderImpl(
            rid,
            pos,
            scheduler.chooseThread(rid.getRangeId()),
            scClient,
            MAX_READAHEAD_BYTES,
            RECONNECT_BACKOFF_MS);
        reader.initialize().whenComplete((value, cause) -> {
            if (null == cause) {
                openFuture.complete(reader);
            } else {
                openFuture.completeExceptionally(cause);
            }
        });
        return openFuture;
    }

}
