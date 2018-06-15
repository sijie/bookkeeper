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
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.junit.Test;

/**
 * Unit test {@link PendingEventSet}.
 */
public class PendingEventSetTest {

    @Test
    public void testComplete() throws Exception {
        ByteBuf dataBuf = Unpooled.copiedBuffer("test-buffer", UTF_8);
        int numEvents = 10;
        List<CompletableFuture<Position>> callbacks =
            IntStream.range(0, numEvents)
                .<CompletableFuture<Position>>mapToObj(
                    idx -> FutureUtils.createFuture())
                .collect(Collectors.toList());
        PendingEventSet eventSet = PendingEventSet.of(dataBuf, callbacks);

        RangePositionImpl rangePos = RangePositionImpl.create(
            1234L,
            3456L,
            7890);
        eventSet.complete(rangePos);

        List<Position> positions = result(FutureUtils.collect(callbacks));
        assertEquals(numEvents, positions.size());
        for (int i = 0; i < numEvents; i++) {
            assertEquals(EventPositionImpl.of(
                1234L,
                3456L,
                7890L,
                i
            ), positions.get(i));
        }

        // both range pos and data buf should be released
        assertEquals(0, dataBuf.refCnt());
        assertEquals(0, rangePos.refCnt());
    }

    @Test
    public void testCompleteExceptionally() {
        ByteBuf dataBuf = Unpooled.copiedBuffer("test-buffer", UTF_8);
        int numEvents = 10;
        List<CompletableFuture<Position>> callbacks =
            IntStream.range(0, numEvents)
                .<CompletableFuture<Position>>mapToObj(
                    idx -> FutureUtils.createFuture())
                .collect(Collectors.toList());
        PendingEventSet eventSet = PendingEventSet.of(dataBuf, callbacks);

        Exception testException = new Exception("test-exception");
        eventSet.completeExceptionally(testException);

        for (int i = 0; i < numEvents; i++) {
            try {
                result(callbacks.get(i));
                fail("Should fail callback '" + i + "'");
            } catch (Exception e) {
                assertSame(testException, e);
            }
        }

        // data buf should be released
        assertEquals(0, dataBuf.refCnt());
    }

}
