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

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.stub.StreamObserver;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.Reader;
import org.apache.bookkeeper.api.stream.ReaderConfig;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.Writer;
import org.apache.bookkeeper.api.stream.WriterConfig;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.coder.VarIntCoder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.router.IntHashRouter;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test {@link StreamImpl}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    StreamImpl.class,
    WriterImpl.class,
    ReaderImpl.class
})
public class StreamImplTest extends GrpcClientTestBase {

    private static final String STREAM_NAME = "test-stream-name";
    private static final StreamProperties STREAM_PROPS = StreamProperties.newBuilder()
        .setStreamId(1234L)
        .setStreamConf(DEFAULT_STREAM_CONF)
        .setStorageContainerId(16)
        .setStreamName(STREAM_NAME)
        .build();

    private StreamConfig<Integer, String> streamConfig;
    private StreamImpl<Integer, String> streamHandle;

    @Override
    protected void doSetup() {
        this.streamConfig = StreamConfig.<Integer, String>builder()
            .keyCoder(VarIntCoder.of())
            .valueCoder(StringUtf8Coder.of())
            .keyRouter(IntHashRouter.of())
            .build();
        this.streamHandle = new StreamImpl<>(
            STREAM_NAME,
            STREAM_PROPS,
            settings,
            streamConfig,
            serverManager,
            scheduler
        );
    }

    @Override
    protected void doTeardown() {
        this.streamHandle.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOpenWriter() throws Exception {
        WriterImpl<Integer, String> writerImpl = mock(WriterImpl.class);
        when(writerImpl.initialize()).thenReturn(FutureUtils.value(writerImpl));
        PowerMockito.whenNew(WriterImpl.class)
            .withAnyArguments()
            .thenReturn(writerImpl);

        WriterConfig writerConfig = WriterConfig.builder().build();
        Writer<Integer, String> returnedWriter =
            FutureUtils.result(streamHandle.openWriter(writerConfig));
        assertSame(writerImpl, returnedWriter);

        verify(writerImpl, times(1)).initialize();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOpenReader() throws Exception {
        ReaderImpl<Integer, String> readerImpl = mock(ReaderImpl.class);
        when(readerImpl.start()).thenReturn(FutureUtils.value(readerImpl));
        PowerMockito.whenNew(ReaderImpl.class)
            .withAnyArguments()
            .thenReturn(readerImpl);

        LocalReadGroupController readGroup = mock(LocalReadGroupController.class);
        when(readGroup.start(any(StreamObserver.class)))
            .thenReturn(FutureUtils.Void());
        PowerMockito.whenNew(LocalReadGroupController.class)
            .withAnyArguments()
            .thenReturn(readGroup);

        ReaderConfig readerConfig = ReaderConfig.builder().build();
        Reader<Integer, String> returnedReader =
            FutureUtils.result(streamHandle.openReader(readerConfig, Position.HEAD));
        assertSame(readerImpl, returnedReader);

        verify(readGroup, times(1)).start(same(readerImpl));
        verify(readerImpl, times(1)).start();
    }
}
