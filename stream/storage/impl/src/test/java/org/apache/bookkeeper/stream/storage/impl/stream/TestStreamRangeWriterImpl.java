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

package org.apache.bookkeeper.stream.storage.impl.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetRequest;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetResponse;
import org.apache.bookkeeper.stream.storage.exceptions.ConditionalWriteException;
import org.apache.bookkeeper.stream.storage.exceptions.UnimplementedException;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.junit.Test;

/**
 * Unit test of {@link StreamRangeReaderImpl}.
 */
public class TestStreamRangeWriterImpl {

    private static final long rid = System.currentTimeMillis();
    private static final long writerId = System.currentTimeMillis() * 2;
    private final AsyncLogWriter logWriter = mock(AsyncLogWriter.class);
    private final OffsetSequencer offsetSequencer = new OffsetSequencer(0L);
    private final StreamRangeImpl streamRange = mock(StreamRangeImpl.class);
    private final StreamRangeWriterImpl writerImpl =
        new StreamRangeWriterImpl(rid, writerId, logWriter, offsetSequencer, streamRange);

    @Test
    public void testGetWriter() {
        assertEquals(logWriter, writerImpl.getWriter());
    }

    @Test
    public void testGetWriterId() {
        assertEquals(writerId, writerImpl.getWriterId());
    }

    @Test
    public void testGetLastEventSetId() {
        assertEquals(-1L, writerImpl.getLastEventSetId());
    }

    @Test
    public void testWriterSessionAborted() {
        writerImpl.onWriteSessionAborted(new Exception("test-exception"));
        verify(logWriter, times(0)).asyncClose();
    }

    @Test
    public void testReadSessionCompleted() {
        writerImpl.onWriteSessionCompleted();
        verify(logWriter, times(0)).asyncClose();
    }

    @Test
    public void testWriteExpectedRangeNum() throws Exception {
        WriteEventSetRequest request = WriteEventSetRequest.newBuilder()
            .setEventSetId(1234L)
            .setExpectedRangeId(rid)
            .setExpectedRangeSeqNum(1L)
            .setData(ByteString.copyFromUtf8("write-expected-range-num"))
            .build();

        try {
            FutureUtils.result(writerImpl.write(request));
            fail("conditional write with expected range seq num should fail with unimplemented exception");
        } catch (UnimplementedException ue) {
            // expected
        }
    }

    @Test
    public void testConditionalWriteWrongRangeId() throws Exception {
        WriteEventSetRequest request = WriteEventSetRequest.newBuilder()
            .setEventSetId(1234L)
            .setExpectedRangeId(rid + 1)
            .setExpectedRangeOffset(0L)
            .setData(ByteString.copyFromUtf8("write-wrong-range-id"))
            .build();

        try {
            FutureUtils.result(writerImpl.write(request));
            fail("conditional write with wrong range id with fail with conditional write exception");
        } catch (ConditionalWriteException cwe) {
            // expected
        }
    }

    @Test
    public void testConditionalWriteWrongExpectedOffset() throws Exception {
        WriteEventSetRequest request = WriteEventSetRequest.newBuilder()
            .setEventSetId(1234L)
            .setExpectedRangeId(rid)
            .setExpectedRangeOffset(1L)
            .setData(ByteString.copyFromUtf8("write-expected-offset"))
            .build();

        try {
            FutureUtils.result(writerImpl.write(request));
            fail("conditional write with expected offset should fail with conditional write exception");
        } catch (ConditionalWriteException cwe) {
            // expected
        }
    }

    @Test
    public void testWriteSuccess() throws Exception {
        ByteString data = ByteString.copyFromUtf8("write-success");
        WriteEventSetRequest request = WriteEventSetRequest.newBuilder()
            .setEventSetId(1234L)
            .setData(data)
            .build();

        long offset = System.currentTimeMillis();
        offsetSequencer.advanceOffset(offset);
        DLSN dlsn = new DLSN(12L, 34L, 0L);

        when(logWriter.write(any(LogRecord.class)))
            .thenReturn(FutureUtils.value(dlsn));

        WriteEventSetResponse response = FutureUtils.result(writerImpl.write(request));
        assertEquals(1234L, response.getEventSetId());
        assertEquals(rid, response.getRangeId());
        assertEquals(offset, response.getRangeOffset());
        assertEquals(DLSNUtils.getRangeSeqNum(dlsn), response.getRangeSeqNum());

        // the offset sequencer should be advanced.
        assertEquals(offset + data.size(), offsetSequencer.currentOffset());
    }

    @Test
    public void testConditionalWriteSuccess() throws Exception {
        long offset = System.currentTimeMillis();
        offsetSequencer.advanceOffset(offset);

        ByteString data = ByteString.copyFromUtf8("write-success");
        WriteEventSetRequest request = WriteEventSetRequest.newBuilder()
            .setEventSetId(1234L)
            .setExpectedRangeId(rid)
            .setExpectedRangeOffset(offset)
            .setData(data)
            .build();

        DLSN dlsn = new DLSN(12L, 34L, 0L);
        when(logWriter.write(any(LogRecord.class)))
            .thenReturn(FutureUtils.value(dlsn));

        WriteEventSetResponse response = FutureUtils.result(writerImpl.write(request));
        assertEquals(1234L, response.getEventSetId());
        assertEquals(rid, response.getRangeId());
        assertEquals(offset, response.getRangeOffset());
        assertEquals(DLSNUtils.getRangeSeqNum(dlsn), response.getRangeSeqNum());

        // the offset sequencer should be advanced.
        assertEquals(offset + data.size(), offsetSequencer.currentOffset());
    }

    @Test
    public void testWriteFailure() throws Exception {
        ByteString data = ByteString.copyFromUtf8("write-success");
        WriteEventSetRequest request = WriteEventSetRequest.newBuilder()
            .setEventSetId(1234L)
            .setData(data)
            .build();

        long offset = System.currentTimeMillis();
        offsetSequencer.advanceOffset(offset);

        IOException ioe = new IOException("test-io-exception");

        when(logWriter.write(any(LogRecord.class)))
            .thenReturn(FutureUtils.exception(ioe));

        try {
            FutureUtils.result(writerImpl.write(request));
            fail("should fail writing event set");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertSame(ioe, e);
        }

        // the offset sequencer is advanced anyway
        assertEquals(offset + data.size(), offsetSequencer.currentOffset());
        // but the writer should be aborted.
        verify(streamRange, times(1)).abortAndResetLogWriter(same(logWriter));
    }

}
