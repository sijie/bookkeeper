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

package org.apache.bookkeeper.clients.impl.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.Reader;
import org.apache.bookkeeper.api.stream.ReaderConfig;
import org.apache.bookkeeper.api.stream.Stream;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.Writer;
import org.apache.bookkeeper.api.stream.WriterConfig;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * The default implementation of a stream handle.
 */
public class StreamImpl<KeyT, ValueT>
        extends AbstractAutoAsyncCloseable
        implements Stream<KeyT, ValueT> {

    private final String nsName;
    private final StreamProperties streamProps;
    private final StorageClientSettings settings;
    private final StreamConfig<KeyT, ValueT> streamConfig;
    private final StorageServerClientManager clientManager;
    private final OrderedScheduler scheduler;

    public StreamImpl(String nsName,
                      StreamProperties streamProps,
                      StorageClientSettings settings,
                      StreamConfig<KeyT, ValueT> streamConfig,
                      StorageServerClientManager clientManager,
                      OrderedScheduler scheduler) {
        this.nsName = nsName;
        this.streamProps = streamProps;
        this.settings = settings;
        this.streamConfig = streamConfig;
        this.clientManager = clientManager;
        this.scheduler = scheduler;
    }

    @Override
    public CompletableFuture<Reader<KeyT, ValueT>> openReader(ReaderConfig config, Position position) {
        String readerName = settings.clientName().orElse("default-reader");

        Map<String, Position> positions = new HashMap<>();
        positions.put(streamProps.getStreamName(), position);

        LocalReadGroup lrg = new LocalReadGroup(
            nsName,
            positions,
            clientManager,
            scheduler.chooseThread());

        ReaderImpl<KeyT, ValueT> reader = new ReaderImpl<>(
            readerName,
            positions,
            streamConfig,
            config,
            clientManager,
            scheduler,
            lrg);
        return lrg.start(reader).thenCompose(ignored -> reader.start());
    }

    @Override
    public CompletableFuture<Writer<KeyT, ValueT>> openWriter(WriterConfig config) {
        String writerName = settings.clientName().orElse("default-writer");

        WriterImpl<KeyT, ValueT> writer = new WriterImpl<>(
            writerName,
            streamProps.getStreamName(),
            streamProps,
            streamConfig,
            config,
            clientManager,
            scheduler);
        return writer.initialize();
    }

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        FutureUtils.complete(closeFuture, null);
    }

    @Override
    public void close() {
        super.close();
    }
}
