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

import static io.netty.util.ReferenceCountUtil.retain;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.api.stream.ReadEvents;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet.ReaderBuilder;
import org.apache.bookkeeper.clients.impl.stream.event.RangePositionImpl;
import org.apache.bookkeeper.clients.impl.stream.event.ReadEventsImpl;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.protocol.RangeId;

/**
 * An implementation for {@link StreamRangeEventReader}.
 */
public class StreamRangeEventReaderImpl<KeyT, ValueT>
    extends AbstractAutoAsyncCloseable
    implements StreamRangeEventReader<KeyT, ValueT> {

    private final StreamRangeClients clients;
    private final String streamName;
    private final RangeId rangeId;
    private final RangeProperties rangeProps;
    private final RangePosition rangePos;
    private final ReaderBuilder<KeyT, ValueT> readerBuilder;
    private final ReadEventsImpl.Recycler<KeyT, ValueT> eventsRecycler;
    private final ScheduledExecutorService executor;

    private StreamRangeEventSetReader eventSetReader;

    public StreamRangeEventReaderImpl(StreamRangeClients clients,
                                      String streamName,
                                      RangeId rangeId,
                                      RangeProperties rangeProps,
                                      RangePosition rangePos,
                                      ReaderBuilder<KeyT, ValueT> readerBuilder,
                                      ReadEventsImpl.Recycler<KeyT, ValueT> eventsRecycler,
                                      ScheduledExecutorService executor) {
        this.clients = clients;
        this.streamName = streamName;
        this.rangeId = rangeId;
        this.rangeProps = rangeProps;
        this.rangePos = rangePos;
        this.readerBuilder = readerBuilder;
        this.eventsRecycler = eventsRecycler;
        this.executor = executor;
    }

    @VisibleForTesting
    StreamRangeEventSetReader getEventSetReader() {
        return eventSetReader;
    }

    @Override
    public RangeId getRangeId() {
        return this.rangeId;
    }

    @Override
    public CompletableFuture<StreamRangeEventReader<KeyT, ValueT>> initialize() {
        return clients.openStreamRangeReader(
            rangeId.getStreamId(),
            rangeProps,
            rangePos
        ).thenApplyAsync(streamRangeEventSetReader -> {
            this.eventSetReader = streamRangeEventSetReader;
            return this;
        }, executor);
    }

    @Override
    public CompletableFuture<ReadEvents<KeyT, ValueT>> readNext() {
        return eventSetReader.readNext().thenApplyAsync(readData -> {
            try {
                ByteBuf data = readData.getData();
                int estimatedSize = data.readableBytes();
                return eventsRecycler.create(
                    streamName,
                    rangeId,
                    readerBuilder.build(retain(readData.getData())),
                    RangePositionImpl.create(
                        readData.getRangeId(),
                        readData.getRangeOffset(),
                        readData.getRangeSeqNum()),
                    estimatedSize);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                readData.release();
            }
        }, executor);
    }

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        if (null != eventSetReader) {
            FutureUtils.proxyTo(
                eventSetReader.closeAsync(),
                closeFuture
            );
        } else {
            closeFuture.complete(null);
        }
    }
}
