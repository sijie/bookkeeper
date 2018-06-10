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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.storage.SetupReaderRequest;
import org.apache.bookkeeper.stream.proto.storage.SetupWriterRequest;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeReader;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeWriter;
import org.apache.distributedlog.api.namespace.Namespace;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link StreamStoreImpl}.
 */
public class TestStreamStoreImpl {

    private static final SetupWriterRequest startWriteRequest = SetupWriterRequest.newBuilder()
        .setStreamId(1234L)
        .setRangeId(123L)
        .setWriterName("test-writer")
        .build();
    private static final SetupReaderRequest startReaderRequest = SetupReaderRequest.newBuilder()
        .setStreamId(1234L)
        .setRangeId(123L)
        .setRangeSeqNum(0L)
        .setReadSessionId(0)
        .build();
    private final StreamRangeImpl rangeImpl = mock(StreamRangeImpl.class);
    private final Namespace namespace = mock(Namespace.class);
    private StreamStoreImpl store;

    @Before
    public void setUp() {
        store = new StreamStoreImpl(
            namespace,
            (sid, rid) -> rangeImpl);
    }

    @Test
    public void testOpenStreamRangeWriterSuccess() throws Exception {
        StreamRangeWriter writer = mock(StreamRangeWriter.class);
        when(rangeImpl.openWriter(any(SetupWriterRequest.class)))
            .thenReturn(FutureUtils.value(writer));

        CompletableFuture<StreamRangeWriter> future = store.openStreamRangeWriter(startWriteRequest);
        StreamRangeWriter createdWriter = FutureUtils.result(future);

        assertEquals(writer, createdWriter);

        verify(rangeImpl, times(1)).openWriter(eq(startWriteRequest));
    }

    @Test
    public void testOpenStreamRangeWriterFailure() throws Exception {
        IOException expected = new IOException("test-exception");

        StreamStoreImpl storeImpl = new StreamStoreImpl(namespace);
        doThrow(expected).when(namespace).openLog(anyString());

        CompletableFuture<StreamRangeWriter> future = storeImpl.openStreamRangeWriter(startWriteRequest);
        try {
            FutureUtils.result(future);
            fail("Should fail on open stream range writer");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertSame(expected, e);
        }
    }

    @Test
    public void testOpenStreamRangeReaderSuccess() throws Exception {
        StreamRangeReader writer = mock(StreamRangeReader.class);
        when(rangeImpl.openReader(any(SetupReaderRequest.class)))
            .thenReturn(FutureUtils.value(writer));

        CompletableFuture<StreamRangeReader> future = store.openStreamRangeReader(startReaderRequest);
        StreamRangeReader createdReader = FutureUtils.result(future);

        assertEquals(writer, createdReader);

        verify(rangeImpl, times(1)).openReader(eq(startReaderRequest));
    }

    @Test
    public void testOpenStreamRangeReaderFailure() throws Exception {
        IOException expected = new IOException("test-exception");

        StreamStoreImpl storeImpl = new StreamStoreImpl(namespace);
        doThrow(expected).when(namespace).openLog(anyString());

        CompletableFuture<StreamRangeReader> future = storeImpl.openStreamRangeReader(startReaderRequest);
        try {
            FutureUtils.result(future);
            fail("Should fail on open stream range reader");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertSame(expected, e);
        }
    }

}
