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

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_WRITER_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.storage.SetupReaderRequest;
import org.apache.bookkeeper.stream.proto.storage.SetupWriterRequest;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeWriter;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.junit.Test;

/**
 * Unit test for {@link StreamRangeImpl}.
 */
public class TestStreamRangeImpl {

    private final long rid = System.currentTimeMillis();
    private final DistributedLogManager manager = mock(DistributedLogManager.class);
    private final StreamRangeImpl rangeImpl = new StreamRangeImpl(rid, manager);
    private final SetupWriterRequest.Builder startWriterReqBuilder = SetupWriterRequest.newBuilder()
        .setStreamId(123L)
        .setRangeId(234L)
        .setWriterName("test-writer");
    private final SetupReaderRequest.Builder startReaderReqBuilder = SetupReaderRequest.newBuilder()
        .setStreamId(123L)
        .setRangeId(234L)
        .setRangeOffset(System.currentTimeMillis())
        .setReadSessionId(1);

    @Test
    public void testGetLastPositionSuccess() throws Exception {
        CompletableFuture<LogRecordWithDLSN> getFuture = new CompletableFuture<>();
        LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);
        DLSN dlsn = new DLSN(System.currentTimeMillis(),
            2 * System.currentTimeMillis(),
            3 * System.currentTimeMillis());
        long offset = System.currentTimeMillis();
        when(record.getTransactionId()).thenReturn(offset);
        when(record.getDlsn()).thenReturn(dlsn);
        when(manager.getLastLogRecordAsync()).thenReturn(getFuture);

        CompletableFuture<RangePosition> future = rangeImpl.getLastPosition();
        getFuture.complete(record);
        RangePosition eventPos = FutureUtils.result(future);
        assertEquals(rid, eventPos.getRangeId());
        assertEquals(offset, eventPos.getOffset());
        assertEquals(DLSNUtils.getRangeSeqNum(dlsn), eventPos.getSeqNum());
    }

    @Test
    public void testGetLastPositionFailure() throws Exception {
        CompletableFuture<LogRecordWithDLSN> getFuture = new CompletableFuture<>();
        when(manager.getLastLogRecordAsync()).thenReturn(getFuture);

        Exception expected = new IOException("test-exception");

        CompletableFuture<RangePosition> future = rangeImpl.getLastPosition();
        getFuture.completeExceptionally(expected);
        try {
            FutureUtils.result(future);
            fail("Should fail on get last position");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertSame(expected, e);
        }
    }

    @Test
    public void testOpenWriterSuccess() throws Exception {
        assertNull(rangeImpl.getWriter());
        assertNull(rangeImpl.getWriterFuture());

        AsyncLogWriter logWriter = mock(AsyncLogWriter.class);
        CompletableFuture<AsyncLogWriter> openPromise = new CompletableFuture<>();
        when(manager.openAsyncLogWriter()).thenReturn(openPromise);

        // concurrent open before #openAsyncLogWriter() completes
        CompletableFuture<StreamRangeWriter> future1 = rangeImpl.openWriter(startWriterReqBuilder.build());
        assertNull(rangeImpl.getWriter());
        CompletableFuture<AsyncLogWriter> openWriterFuture1 = rangeImpl.getWriterFuture();
        assertNotNull(openWriterFuture1);
        CompletableFuture<StreamRangeWriter> future2 = rangeImpl.openWriter(startWriterReqBuilder.build());
        assertNull(rangeImpl.getWriter());
        CompletableFuture<AsyncLogWriter> openWriterFuture2 = rangeImpl.getWriterFuture();
        assertNotNull(openWriterFuture2);
        assertTrue(openWriterFuture1 == openWriterFuture2);

        // complete the open operation
        openPromise.complete(logWriter);

        StreamRangeWriterImpl writer1 = (StreamRangeWriterImpl) FutureUtils.result(future1);
        StreamRangeWriterImpl writer2 = (StreamRangeWriterImpl) FutureUtils.result(future2);
        assertTrue(writer1.getWriter() == writer2.getWriter());
        assertTrue(logWriter == writer1.getWriter());
        assertNotNull(rangeImpl.getWriter());
        assertTrue(logWriter == rangeImpl.getWriter());

        assertEquals(INVALID_WRITER_ID, writer1.getWriterId());
        assertEquals(INVALID_WRITER_ID, writer2.getWriterId());

        // open another writer
        CompletableFuture<StreamRangeWriter> future3 = rangeImpl.openWriter(startWriterReqBuilder.build());
        StreamRangeWriterImpl writer3 = (StreamRangeWriterImpl) FutureUtils.result(future3);
        assertTrue(writer3.getWriter() == logWriter);
        assertEquals(INVALID_WRITER_ID, writer3.getWriterId());

        // the underlying open should be called only once
        verify(manager, times(1)).openAsyncLogWriter();
    }

    @Test
    public void testOpenWriteFailure() throws Exception {
        AsyncLogWriter logWriter = mock(AsyncLogWriter.class);
        CompletableFuture<AsyncLogWriter> openPromise1 = new CompletableFuture<>();
        CompletableFuture<AsyncLogWriter> openPromise2 = new CompletableFuture<>();
        when(manager.openAsyncLogWriter())
            .thenReturn(openPromise1)
            .thenReturn(openPromise2);

        // open the writer
        CompletableFuture<StreamRangeWriter> future1 = rangeImpl.openWriter(startWriterReqBuilder.build());
        assertNull(rangeImpl.getWriter());
        assertNotNull(rangeImpl.getWriterFuture());
        Exception expected = new IOException("test-exception");
        try {
            openPromise1.completeExceptionally(expected);
            FutureUtils.result(future1);
            fail("Should fail opening writer.");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertSame(expected, e);
        }
        assertNull(rangeImpl.getWriter());
        assertNull(rangeImpl.getWriterFuture());

        // open another writer
        CompletableFuture<StreamRangeWriter> future2 = rangeImpl.openWriter(startWriterReqBuilder.build());
        assertNull(rangeImpl.getWriter());
        assertNotNull(rangeImpl.getWriterFuture());
        openPromise2.complete(logWriter);
        StreamRangeWriterImpl writer2 = (StreamRangeWriterImpl) FutureUtils.result(future2);
        assertTrue(writer2.getWriter() == logWriter);
        assertEquals(INVALID_WRITER_ID, writer2.getWriterId());

        // the underlying open should be called twice
        verify(manager, times(2)).openAsyncLogWriter();
    }

    @Test
    public void testClose() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        closeFuture.complete(null);
        when(writer.asyncClose()).thenReturn(closeFuture);
        CompletableFuture<AsyncLogWriter> openFuture = new CompletableFuture<>();
        openFuture.complete(writer);
        when(manager.openAsyncLogWriter()).thenReturn(openFuture);

        // open writer
        CompletableFuture<StreamRangeWriter> future = rangeImpl.openWriter(startWriterReqBuilder
            .build());
        StreamRangeWriterImpl writerImpl = (StreamRangeWriterImpl) FutureUtils.result(future);
        assertEquals(writer, writerImpl.getWriter());
        assertEquals(INVALID_WRITER_ID, writerImpl.getWriterId());

        rangeImpl.close();
        verify(writer, times(1)).asyncClose();
        verify(manager, times(1)).close();
        assertNotNull(rangeImpl.getWriter());
        assertNotNull(rangeImpl.getWriterFuture());
    }

    @Test
    public void testAbortAndResetLogWriter() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        closeFuture.complete(null);
        when(writer.asyncClose()).thenReturn(closeFuture);
        CompletableFuture<AsyncLogWriter> openFuture = new CompletableFuture<>();
        openFuture.complete(writer);
        when(manager.openAsyncLogWriter()).thenReturn(openFuture);

        // open writer
        CompletableFuture<StreamRangeWriter> future = rangeImpl.openWriter(startWriterReqBuilder.build());
        StreamRangeWriterImpl writerImpl = (StreamRangeWriterImpl) FutureUtils.result(future);
        assertEquals(writer, writerImpl.getWriter());
        assertEquals(INVALID_WRITER_ID, writerImpl.getWriterId());

        rangeImpl.abortAndResetLogWriter(writer);
        verify(writer, times(1)).asyncClose();
        assertNull(rangeImpl.getWriter());
        assertNull(rangeImpl.getWriterFuture());
    }

    @Test
    public void testAbortAndResetLogWriter2() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        closeFuture.complete(null);
        when(writer.asyncClose()).thenReturn(closeFuture);
        CompletableFuture<AsyncLogWriter> openFuture = new CompletableFuture<>();
        openFuture.complete(writer);
        when(manager.openAsyncLogWriter()).thenReturn(openFuture);

        // open writer
        CompletableFuture<StreamRangeWriter> future = rangeImpl.openWriter(startWriterReqBuilder.build());
        StreamRangeWriterImpl writerImpl = (StreamRangeWriterImpl) FutureUtils.result(future);
        assertEquals(writer, writerImpl.getWriter());
        assertEquals(INVALID_WRITER_ID, writerImpl.getWriterId());

        AsyncLogWriter writer2 = mock(AsyncLogWriter.class);
        when(writer2.asyncClose()).thenReturn(closeFuture);
        rangeImpl.abortAndResetLogWriter(writer2);
        verify(writer, times(0)).asyncClose();
        assertNotNull(rangeImpl.getWriter());
        assertNotNull(rangeImpl.getWriterFuture());
    }

    @Test
    public void testOpenReaderSuccess() throws Exception {
        AsyncLogReader logReader = mock(AsyncLogReader.class);
        CompletableFuture<AsyncLogReader> openFuture = new CompletableFuture<>();
        when(manager.openAsyncLogReader(any(DLSN.class)))
            .thenReturn(openFuture);

        DLSN dlsn = new DLSN(1234L, 4567L, 0L);
        long seqNum = DLSNUtils.getRangeSeqNum(dlsn);

        SetupReaderRequest request = SetupReaderRequest.newBuilder(startReaderReqBuilder.build())
            .setRangeSeqNum(seqNum)
            .build();
        openFuture.complete(logReader);
        StreamRangeReaderImpl reader = (StreamRangeReaderImpl) FutureUtils.result(rangeImpl.openReader(request));
        assertEquals(logReader, reader.getReader());
        verify(manager, times(1)).openAsyncLogReader(eq(dlsn));
    }

    @Test
    public void testOpenReaderFailure() throws Exception {
        CompletableFuture<AsyncLogReader> openFuture = new CompletableFuture<>();
        when(manager.openAsyncLogReader(any(DLSN.class)))
            .thenReturn(openFuture);

        DLSN dlsn = new DLSN(1234L, 4567L, 0L);
        long seqNum = DLSNUtils.getRangeSeqNum(dlsn);

        SetupReaderRequest request = SetupReaderRequest.newBuilder(startReaderReqBuilder.build())
            .setRangeSeqNum(seqNum)
            .build();
        Exception expected = new IOException("test-exception");
        openFuture.completeExceptionally(expected);
        try {
            FutureUtils.result(rangeImpl.openReader(request));
            fail("Should fail on opening reader");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertSame(expected, e);
        }

        verify(manager, times(1)).openAsyncLogReader(eq(dlsn));
    }

}
