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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_OFFSET;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_RANGE_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_SEQ_NUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

/**
 * Unit test {@link ReadDataImpl}.
 */
public class ReadDataImplTest {

    private static void verifyReadDataNotRecycled(ReadDataImpl readData,
                                                  long seed,
                                                  ByteBuf data) {
        assertEquals(seed, readData.getRangeId());
        assertEquals(2 * seed, readData.getRangeOffset());
        assertEquals(3 * seed, readData.getRangeSeqNum());
        assertSame(data, readData.getData());
        assertEquals(1, readData.getData().refCnt());
    }

    private static void verifyReadDataRecycled(ReadDataImpl readData) {
        assertEquals(INVALID_RANGE_ID, readData.getRangeId());
        assertEquals(INVALID_OFFSET, readData.getRangeOffset());
        assertEquals(INVALID_SEQ_NUM, readData.getRangeSeqNum());
        assertNull(readData.getData());

        ReadDataImpl recycledReadData = ReadDataImpl.create();
        assertSame(recycledReadData, readData);
        recycledReadData.release();
    }

    @Test
    public void testCreateReadData() {
        long seed = System.currentTimeMillis();
        ByteBuf data = Unpooled.copiedBuffer("test-read-data", UTF_8);

        ReadDataImpl readData = ReadDataImpl.create(
            seed,
            2 * seed,
            3 * seed,
            data
        );
        assertEquals(1, readData.refCnt());
        verifyReadDataNotRecycled(readData, seed, data);

        // retain reference will not increase reference of the underlying data
        readData.retain();
        verifyReadDataNotRecycled(readData, seed, data);
        assertEquals(2, readData.refCnt());

        // release reference will not decrease reference of the underlying data
        readData.release();
        verifyReadDataNotRecycled(readData, seed, data);
        assertEquals(1, readData.refCnt());

        // release reference will deallocate the read data instance and hence
        // it will release the underlying buffer
        readData.release();
        verifyReadDataRecycled(readData);
        assertEquals(0, readData.refCnt());
        // the underlying buffer should be released.
        assertEquals(0, data.refCnt());
    }

}
