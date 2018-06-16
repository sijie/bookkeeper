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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannelManager;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeClients.StreamRangeEventSetReaderFactory;
import org.apache.bookkeeper.clients.impl.stream.range.StreamRangeClients.StreamRangeEventSetWriterFactory;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit test {@link StreamRangeClients}.
 */
@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class StreamRangeClientsTest {

    @Rule
    public final TestName runtime = new TestName();

    private OrderedScheduler scheduler;
    private StorageContainerChannelManager mockChannelManager;
    private StreamRangeEventSetWriterFactory mockWriterFactory;
    private StreamRangeEventSetReaderFactory mockReaderFactory;
    private StreamRangeClients streamRangeClients;

    @Before
    public void setup() {
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-scheduler")
            .numThreads(1)
            .build();
        mockChannelManager = mock(StorageContainerChannelManager.class);
        mockWriterFactory = mock(StreamRangeEventSetWriterFactory.class);
        mockReaderFactory = mock(StreamRangeEventSetReaderFactory.class);
        streamRangeClients = new StreamRangeClients(
            mockChannelManager,
            scheduler,
            mockWriterFactory,
            mockReaderFactory);
    }

    @After
    public void teardown() {
        scheduler.shutdown();
    }

    @Test
    public void testOpenStreamRangeWriterSuccess() throws Exception {
        long streamId = System.currentTimeMillis();
        long rangeId = System.currentTimeMillis() * 3;
        long scId = System.currentTimeMillis() / 2;
        String writerName = runtime.getMethodName();
        RangeProperties rangeProps = RangeProperties.newBuilder()
            .setRangeId(rangeId)
            .setStartHashKey(Long.MAX_VALUE)
            .setEndHashKey(Long.MIN_VALUE)
            .setStorageContainerId(scId)
            .build();

        StorageContainerChannel channel = mock(StorageContainerChannel.class);
        when(mockChannelManager.getOrCreate(anyLong()))
            .thenReturn(channel);

        StreamRangeEventSetWriter eventSetWriter = mock(StreamRangeEventSetWriter.class);
        CompletableFuture<StreamRangeEventSetWriter> writerFuture = FutureUtils.value(eventSetWriter);
        when(eventSetWriter.initialize()).thenReturn(writerFuture);

        when(mockWriterFactory.newEventSetWriter(
            any(),
            any(),
            any(),
            any()
        )).thenReturn(eventSetWriter);

        StreamRangeEventSetWriter openEventSetWriter = FutureUtils.result(
            streamRangeClients.openStreamRangeWriter(streamId, rangeProps, writerName));
        assertSame(eventSetWriter, openEventSetWriter);

        verify(mockChannelManager, times(1)).getOrCreate(eq(scId));
        verify(
            mockWriterFactory,
            times(1)
        ).newEventSetWriter(
            eq(RangeId.of(streamId, rangeId)),
            eq(writerName),
            any(ScheduledExecutorService.class),
            same(channel));
        verify(eventSetWriter, times(1)).initialize();
    }

    @Test
    public void testOpenStreamRangeWriterFailure() {
        long streamId = System.currentTimeMillis();
        long rangeId = System.currentTimeMillis() * 3;
        long scId = System.currentTimeMillis() / 2;
        String writerName = runtime.getMethodName();
        RangeProperties rangeProps = RangeProperties.newBuilder()
            .setRangeId(rangeId)
            .setStartHashKey(Long.MAX_VALUE)
            .setEndHashKey(Long.MIN_VALUE)
            .setStorageContainerId(scId)
            .build();

        StorageContainerChannel channel = mock(StorageContainerChannel.class);
        when(mockChannelManager.getOrCreate(anyLong()))
            .thenReturn(channel);

        StreamRangeEventSetWriter eventSetWriter = mock(StreamRangeEventSetWriter.class);
        Throwable exception = new Exception("test-exception");
        when(eventSetWriter.initialize()).thenReturn(FutureUtils.exception(exception));

        when(mockWriterFactory.newEventSetWriter(
            any(),
            any(),
            any(),
            any()
        )).thenReturn(eventSetWriter);

        try {
            FutureUtils.result(
                streamRangeClients.openStreamRangeWriter(streamId, rangeProps, writerName));
            fail("Should fail to open stream range writer at " + RangeId.of(streamId, rangeProps.getRangeId()));
        } catch (Exception e) {
            assertSame(exception, e);
        }

        verify(mockChannelManager, times(1)).getOrCreate(eq(scId));
        verify(
            mockWriterFactory,
            times(1)
        ).newEventSetWriter(
            eq(RangeId.of(streamId, rangeId)),
            eq(writerName),
            any(ScheduledExecutorService.class),
            same(channel));
        verify(eventSetWriter, times(1)).initialize();
    }

    @Test
    public void testOpenStreamRangeReaderSuccess() throws Exception {
        long streamId = System.currentTimeMillis();
        long rangeId = System.currentTimeMillis() * 3;
        long scId = System.currentTimeMillis() / 2;
        RangeProperties rangeProps = RangeProperties.newBuilder()
            .setRangeId(rangeId)
            .setStartHashKey(Long.MAX_VALUE)
            .setEndHashKey(Long.MIN_VALUE)
            .setStorageContainerId(scId)
            .build();

        StorageContainerChannel channel = mock(StorageContainerChannel.class);
        when(mockChannelManager.getOrCreate(anyLong()))
            .thenReturn(channel);

        StreamRangeEventSetReader eventSetReader = mock(StreamRangeEventSetReader.class);
        CompletableFuture<Void> readerFuture = FutureUtils.value(null);
        when(eventSetReader.initialize()).thenReturn(readerFuture);

        when(mockReaderFactory.newEventSetReader(
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong()
        )).thenReturn(eventSetReader);

        RangePosition rangePos = RangePosition.newBuilder().build();
        StreamRangeEventSetReader openEventSetReader = FutureUtils.result(
            streamRangeClients.openStreamRangeReader(
                streamId,
                rangeProps,
                rangePos));
        assertSame(eventSetReader, openEventSetReader);

        verify(mockChannelManager, times(1)).getOrCreate(eq(scId));
        verify(
            mockReaderFactory,
            times(1)
        ).newEventSetReader(
            eq(RangeId.of(streamId, rangeId)),
            same(rangePos),
            any(ScheduledExecutorService.class),
            same(channel),
            anyLong(),
            anyLong());
        verify(eventSetReader, times(1)).initialize();
    }

    @Test
    public void testOpenStreamRangeReaderFailure() {
        long streamId = System.currentTimeMillis();
        long rangeId = System.currentTimeMillis() * 3;
        long scId = System.currentTimeMillis() / 2;
        RangeProperties rangeProps = RangeProperties.newBuilder()
            .setRangeId(rangeId)
            .setStartHashKey(Long.MAX_VALUE)
            .setEndHashKey(Long.MIN_VALUE)
            .setStorageContainerId(scId)
            .build();

        StorageContainerChannel channel = mock(StorageContainerChannel.class);
        when(mockChannelManager.getOrCreate(anyLong()))
            .thenReturn(channel);

        StreamRangeEventSetReader eventSetReader = mock(StreamRangeEventSetReader.class);
        Throwable exception = new Exception("test-exception");
        when(eventSetReader.initialize()).thenReturn(FutureUtils.exception(exception));

        when(mockReaderFactory.newEventSetReader(
            any(),
            any(),
            any(),
            any(),
            anyLong(),
            anyLong()
        )).thenReturn(eventSetReader);

        RangePosition rangePos = RangePosition.newBuilder().build();
        try {
            FutureUtils.result(
                streamRangeClients.openStreamRangeReader(
                    streamId,
                    rangeProps,
                    rangePos));
            fail("Should fail to open stream range reader");
        } catch (Exception e) {
            assertSame(exception, e);
        }

        verify(mockChannelManager, times(1)).getOrCreate(eq(scId));
        verify(
            mockReaderFactory,
            times(1)
        ).newEventSetReader(
            eq(RangeId.of(streamId, rangeId)),
            same(rangePos),
            any(ScheduledExecutorService.class),
            same(channel),
            anyLong(),
            anyLong());
        verify(eventSetReader, times(1)).initialize();
    }

}
