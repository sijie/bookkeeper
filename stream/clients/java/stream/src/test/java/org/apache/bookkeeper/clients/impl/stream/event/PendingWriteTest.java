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

package org.apache.bookkeeper.clients.impl.stream.event;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.clients.impl.stream.exceptions.InternalStreamException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.junit.Test;

/**
 * Unit test {@link PendingWrite}.
 */
public class PendingWriteTest {

    private static PendingWrite createPendingWrite(long sequenceId,
                                                   ByteBuf data,
                                                   CompletableFuture<RangePositionImpl> writeFuture) {
        return PendingWrite.of(
            sequenceId,
            PendingEventSet.of(data, Lists.newArrayList()),
            writeFuture);
    }

    @Test
    public void testConstructor() {
        byte[] data = "test-constructor".getBytes(UTF_8);
        ByteBuf dataBuf = Unpooled.wrappedBuffer(data);
        CompletableFuture<RangePositionImpl> writeFuture = FutureUtils.createFuture();
        PendingWrite write = createPendingWrite(1234L, dataBuf, writeFuture);
        assertSame(data, write.getEventSet().getEventSetBuf().array());
        assertEquals(1234L, write.getSequenceId());
        assertSame(writeFuture, write.getWriteFuture());
        assertFalse(write.isDone());
    }

    @Test
    public void testSetDone() throws Exception {
        byte[] data = "test-constructor".getBytes(UTF_8);
        ByteBuf dataBuf = Unpooled.wrappedBuffer(data);
        CompletableFuture<RangePositionImpl> writeFuture = FutureUtils.createFuture();
        PendingWrite write = createPendingWrite(1234L, dataBuf, writeFuture);
        assertArrayEquals(data, write.getEventSet().getEventSetBuf().array());
        assertEquals(1234L, write.getSequenceId());
        assertTrue(writeFuture == write.getWriteFuture());
        assertFalse(write.isDone());

        // set the write to be done but not submit the callback
        RangePositionImpl rangePos = RangePositionImpl.create(
            1234L, 5678L, 7890L);
        write.setRangePosition(rangePos);
        assertNull(write.getCause());
        assertFalse(write.getWriteFuture().isDone());
        assertFalse(write.getWriteFuture().isCompletedExceptionally());
        // submit the callbacks
        write.submitCallback();
        RangePositionImpl writeSequece = FutureUtils.result(writeFuture);
        assertSame(rangePos, writeSequece);
        assertTrue(write.getWriteFuture().isDone());
        assertFalse(write.getWriteFuture().isCompletedExceptionally());
        assertTrue(write.isDone());

        // the byte buf is not released after callback is completed.
        assertEquals(1, dataBuf.refCnt());
    }

    @Test
    public void testSetFailure() throws Exception {
        byte[] data = "test-constructor".getBytes(UTF_8);
        ByteBuf dataBuf = Unpooled.wrappedBuffer(data);
        CompletableFuture<RangePositionImpl> writeFuture = FutureUtils.createFuture();
        PendingWrite write = createPendingWrite(1234L, dataBuf, writeFuture);
        assertArrayEquals(data, write.getEventSet().getEventSetBuf().array());
        assertEquals(1234L, write.getSequenceId());
        assertSame(writeFuture, write.getWriteFuture());
        assertFalse(write.isDone());

        // set the write to be failed but not submit the callback
        InternalStreamException testException = new InternalStreamException(
            StatusCode.FAILURE,
            "Test-Exception");
        write.setCause(testException);
        assertNull(write.getRangePosition());
        assertSame(testException, write.getCause());
        assertFalse(write.getWriteFuture().isDone());
        assertFalse(write.getWriteFuture().isCompletedExceptionally());
        // submit the callbacks
        write.submitCallback();
        try {
            FutureUtils.result(writeFuture);
            fail("Should fail the write since it is set to failed");
        } catch (InternalStreamException ise) {
            assertSame(ise, testException);
        }
        assertTrue(write.getWriteFuture().isDone());
        assertTrue(write.getWriteFuture().isCompletedExceptionally());
        assertTrue(write.isDone());
    }

}
