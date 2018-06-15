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

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_OFFSET;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_RANGE_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_SEQ_NUM;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;

/**
 * The internal range position representation.
 */
@InterfaceAudience.Private
public class RangePositionImpl extends AbstractReferenceCounted {

    /**
     * Create a range position instance.
     *
     * @param rangeId range id
     * @param rangeOffset range offset
     * @param rangeSeqNum range seq num
     * @return range position instance
     */
    public static RangePositionImpl create(long rangeId,
                                           long rangeOffset,
                                           long rangeSeqNum) {
        RangePositionImpl pos = RECYCLER.get();
        pos.setRefCnt(1);
        pos.rangeId = rangeId;
        pos.rangeOffset = rangeOffset;
        pos.rangeSeqNum = rangeSeqNum;
        return pos;
    }

    private static final Recycler<RangePositionImpl> RECYCLER = new Recycler<RangePositionImpl>() {
        @Override
        protected RangePositionImpl newObject(Handle<RangePositionImpl> handle) {
            return new RangePositionImpl(handle);
        }
    };

    private final Handle<RangePositionImpl> handle;
    private long rangeId;
    private long rangeOffset;
    private long rangeSeqNum;

    private RangePositionImpl(Handle<RangePositionImpl> handle) {
        this.handle = handle;
    }

    public long getRangeId() {
        return rangeId;
    }

    public long getRangeOffset() {
        return rangeOffset;
    }

    public long getRangeSeqNum() {
        return rangeSeqNum;
    }

    @Override
    public RangePositionImpl retain() {
        super.retain();
        return this;
    }

    @Override
    public RangePositionImpl retain(int increment) {
        super.retain(increment);
        return this;
    }


    @Override
    protected void deallocate() {
        rangeId = INVALID_RANGE_ID;
        rangeOffset = INVALID_OFFSET;
        rangeSeqNum = INVALID_SEQ_NUM;
        if (null != handle) {
            handle.recycle(this);
        }
    }

    @Override
    public RangePositionImpl touch() {
        return touch(null);
    }

    @Override
    public RangePositionImpl touch(Object hint) {
        return this;
    }
}
