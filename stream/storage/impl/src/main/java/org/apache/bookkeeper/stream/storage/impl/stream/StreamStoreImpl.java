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

package org.apache.bookkeeper.stream.storage.impl.stream;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.storage.SetupReaderRequest;
import org.apache.bookkeeper.stream.proto.storage.SetupWriterRequest;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeReader;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeWriter;
import org.apache.bookkeeper.stream.storage.api.stream.StreamStore;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;

/**
 * The default implementation of {@link StreamStore}.
 */
public class StreamStoreImpl implements StreamStore {

    static String getLogNameForRange(long streamId, long rangeId) {
        return String.format("stream_%d_range_%d", streamId, rangeId);
    }

    interface StreamRangeFactory {

        StreamRangeImpl create(long streamId, long rangeId) throws IOException;

    }

    private final Namespace namespace;
    private final StreamRangeFactory streamRangeFactory;
    private final ConcurrentLongHashMap<ConcurrentLongHashMap<StreamRangeImpl>> ranges = new ConcurrentLongHashMap<>();

    public StreamStoreImpl(Namespace namespace) {
        this(namespace, (streamId, rid) -> {
            DistributedLogManager manager = namespace.openLog(getLogNameForRange(streamId, rid));
            return new StreamRangeImpl(rid, manager);
        });
    }

    StreamStoreImpl(Namespace namespace, StreamRangeFactory streamRangeFactory) {
        this.namespace = namespace;
        this.streamRangeFactory = streamRangeFactory;
    }

    //
    // Write and Read Operations
    //

    @Override
    public CompletableFuture<StreamRangeWriter> openStreamRangeWriter(SetupWriterRequest request) {
        try {
            return getStreamRange(request.getStreamId(), request.getRangeId()).openWriter(request);
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
    }

    @Override
    public CompletableFuture<StreamRangeReader> openStreamRangeReader(SetupReaderRequest request) {
        try {
            return getStreamRange(request.getStreamId(), request.getRangeId()).openReader(request);
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
    }

    private ConcurrentLongHashMap<StreamRangeImpl> getStreamRanges(long streamId) {
        ConcurrentLongHashMap<StreamRangeImpl> streamRanges = ranges.get(streamId);
        if (null != streamRanges) {
            return streamRanges;
        }
        streamRanges = new ConcurrentLongHashMap<>();
        ConcurrentLongHashMap<StreamRangeImpl> oldStreamRanges = ranges.putIfAbsent(streamId, streamRanges);
        if (null != oldStreamRanges) {
            return oldStreamRanges;
        } else {
            return streamRanges;
        }
    }

    private StreamRangeImpl getStreamRange(long streamId, long rangeId) throws IOException {
        StreamRangeImpl range = getStreamRanges(streamId).get(rangeId);
        if (null != range) {
            return range;
        }
        StreamRangeImpl newRange = streamRangeFactory.create(streamId, rangeId);
        StreamRangeImpl oldRange = getStreamRanges(streamId).putIfAbsent(rangeId, newRange);
        if (null != oldRange) {
            newRange.close();
            return oldRange;
        } else {
            return newRange;
        }
    }
}
