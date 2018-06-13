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

package org.apache.bookkeeper.clients.impl.stream.range;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_OFFSET;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_RANGE_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_SEQ_NUM;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

/**
 * The default implementation of {@link ReadData}.
 */
public class ReadDataImpl
        extends AbstractReferenceCounted
        implements ReadData {

    /**
     * Create a read data instance of provided <tt>data</tt> and its range position.
     *
     * @param rangeId range id
     * @param rangeOffset range offset
     * @param rangeSeqNum range seq num
     * @param data data instance
     * @return read data instance
     */
    public static ReadDataImpl create(long rangeId,
                                      long rangeOffset,
                                      long rangeSeqNum,
                                      ByteBuf data) {
        ReadDataImpl readData = RECYCLER.get();
        readData.setRefCnt(1);
        readData.rangeId = rangeId;
        readData.rangeOffset = rangeOffset;
        readData.rangeSeqNum = rangeSeqNum;
        readData.data = data;
        return readData;
    }

    private static final Recycler<ReadDataImpl> RECYCLER = new Recycler<ReadDataImpl>() {
        @Override
        protected ReadDataImpl newObject(Handle<ReadDataImpl> handle) {
            return new ReadDataImpl(handle);
        }
    };

    private final Handle<ReadDataImpl> handle;
    private ByteBuf data;
    private long rangeId;
    private long rangeOffset;
    private long rangeSeqNum;

    private ReadDataImpl(Handle<ReadDataImpl> recyclerHandle) {
        this.handle = recyclerHandle;
    }

    @Override
    public ReadDataImpl retain() {
        super.retain();
        return this;
    }

    @Override
    public ReadDataImpl retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public ReadDataImpl touch() {
        return touch(null);
    }

    @Override
    public ReadDataImpl touch(Object hint) {
        if (null != data) {
            data.touch(hint);
        }
        return this;
    }

    @Override
    protected void deallocate() {
        if (null != data) {
            data.release();
            data = null;
        }
        rangeId = INVALID_RANGE_ID;
        rangeOffset = INVALID_OFFSET;
        rangeSeqNum = INVALID_SEQ_NUM;
        handle.recycle(this);
    }

    @Override
    public ByteBuf getData() {
        return data;
    }

    @Override
    public long getRangeId() {
        return rangeId;
    }

    @Override
    public long getRangeOffset() {
        return rangeOffset;
    }

    @Override
    public long getRangeSeqNum() {
        return rangeSeqNum;
    }
}
