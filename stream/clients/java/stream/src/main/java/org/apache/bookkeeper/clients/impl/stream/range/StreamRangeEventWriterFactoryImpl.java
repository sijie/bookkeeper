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

package org.apache.bookkeeper.clients.impl.stream.range;

import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.WriterConfig;
import org.apache.bookkeeper.clients.impl.stream.event.EventSet;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.CompressionCodecType;
import org.apache.bookkeeper.stream.proto.EventSetType;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * Default implementation of {@link StreamRangeEventWriter}.
 */
public class StreamRangeEventWriterFactoryImpl implements StreamRangeEventWriterFactory {

    private final StreamRangeClients rangeClients;
    private final OrderedScheduler scheduler;

    public StreamRangeEventWriterFactoryImpl(StreamRangeClients rangeClients,
                                             OrderedScheduler scheduler) {
        this.rangeClients = rangeClients;
        this.scheduler = scheduler;
    }

    @Override
    public <KeyT, ValueT> StreamRangeEventWriter<KeyT, ValueT> createRangeEventWriter(
        String writerName,
        StreamConfig<KeyT, ValueT> streamConfig,
        StreamProperties streamProps,
        RangeProperties rangeProps,
        WriterConfig writerConfig) {

        EventSet.WriterBuilder<KeyT, ValueT> esWriterBuilder = EventSet.<KeyT, ValueT>newWriterBuilder()
            .withEventSetType(EventSetType.DATA)
            .withBufferSize(writerConfig.maxBufferSize())
            .withCompressionCodec(CompressionCodecType.NONE)
            .withKeyCoder(streamConfig.keyCoder())
            .withValueCoder(streamConfig.valueCoder())
            .withMaxNumEvents(writerConfig.maxBufferedEvents());

        return new StreamRangeEventWriterImpl<>(
            rangeClients,
            streamProps.getStreamId(),
            rangeProps,
            writerName,
            esWriterBuilder,
            writerConfig.flushDuration(),
            scheduler.chooseThread(streamProps.getStreamId())
        );
    }
}
