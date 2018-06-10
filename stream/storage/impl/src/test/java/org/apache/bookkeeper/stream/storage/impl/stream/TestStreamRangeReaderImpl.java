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

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.storage.ReadEventSetResponse;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.junit.Test;

/**
 * Unit test of {@link StreamRangeReaderImpl}.
 */
public class TestStreamRangeReaderImpl {

    private static final long rid = System.currentTimeMillis();
    private final AsyncLogReader logReader = mock(AsyncLogReader.class);
    private final StreamRangeReaderImpl readerImpl = new StreamRangeReaderImpl(rid, logReader);

    @Test
    public void testGetReader() {
        assertEquals(logReader, readerImpl.getReader());
    }

    @Test
    public void testReadSessionAborted() {
        readerImpl.onReadSessionAborted(new Exception("test-exception"));
        verify(logReader, times(1)).asyncClose();
    }

    @Test
    public void testReadSessionCompleted() {
        readerImpl.onReadSessionCompleted();
        verify(logReader, times(1)).asyncClose();
    }

    @Test
    public void testReadNext() throws Exception {
        LogRecordWithDLSN record = mock(LogRecordWithDLSN.class);
        CompletableFuture<LogRecordWithDLSN> readNextFuture = new CompletableFuture<>();
        DLSN dlsn = new DLSN(12L, 34L, 56L);
        byte[] data = "test-data".getBytes(UTF_8);
        long offset = ThreadLocalRandom.current().nextLong(1024 * 1024);
        when(record.getTransactionId()).thenReturn(offset);
        when(record.getDlsn()).thenReturn(dlsn);
        when(record.getPayloadBuf()).thenReturn(Unpooled.wrappedBuffer(data));
        when(logReader.readNext()).thenReturn(readNextFuture);

        CompletableFuture<ReadEventSetResponse> readFuture = readerImpl.readNext();
        readNextFuture.complete(record);
        ReadEventSetResponse resp = FutureUtils.result(readFuture);
        assertEquals(ByteString.copyFrom(data), resp.getData());
        assertEquals(rid, resp.getRangeId());
        assertEquals(offset, resp.getRangeOffset());
        assertEquals(DLSNUtils.getRangeSeqNum(dlsn), resp.getRangeSeqNum());

        verify(record, times(1)).getDlsn();
        verify(record, times(1)).getPayloadBuf();
        verify(record, times(1)).getTransactionId();
        verify(logReader, times(1)).readNext();
    }

    @Test
    public void testReadNextEndOfStream() throws Exception {
        CompletableFuture<LogRecordWithDLSN> readNextFuture = new CompletableFuture<>();
        when(logReader.readNext()).thenReturn(readNextFuture);

        EndOfStreamException dlException = new EndOfStreamException("test-exception");

        CompletableFuture<ReadEventSetResponse> readFuture = readerImpl.readNext();
        readNextFuture.completeExceptionally(dlException);

        try {
            FutureUtils.result(readFuture);
            fail("Should fail on hitting end of stream");
        } catch (Exception e) {
            assertTrue("Expected EndOfStreamException : " + e,
                e instanceof EndOfStreamException);
        }
    }

    @Test
    public void testReadNextFailure() throws Exception {
        CompletableFuture<LogRecordWithDLSN> readNextFuture = new CompletableFuture<>();
        when(logReader.readNext()).thenReturn(readNextFuture);

        Exception expected = new IOException("test-exception");

        CompletableFuture<ReadEventSetResponse> readFuture = readerImpl.readNext();
        readNextFuture.completeExceptionally(expected);

        try {
            FutureUtils.result(readFuture);
            fail("Should fail on hitting exceptions");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertSame(expected, e);
        }
    }

}
