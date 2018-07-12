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

package org.apache.bookkeeper.common.coder;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.VarInt;

/**
 * A coder that prepend length before encoded bytes.
 */
@Slf4j
public class LengthPrependerCoder<T> implements Coder<T> {

    private static final long serialVersionUID = -1418224378236449241L;

    /**
     * Create a coder that prepend length field before encoded bytes encoded by provided <tt>coder</tt>.
     */
    public static final <T> LengthPrependerCoder<T> of(Coder<T> coder) {
        if (null == coder) {
            return null;
        } else {
            return new LengthPrependerCoder<>(coder);
        }
    }

    private final Coder<T> coder;

    private LengthPrependerCoder(Coder<T> coder) {
        this.coder = coder;
    }

    @Override
    public void encode(T value, ByteBuf destBuf) {
        if (coder.isLengthRequiredOnNestedContext()) {
            int len = coder.getSerializedSize(value);
            try {
                VarInt.encode(len, destBuf);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to encode length '" + len + "' into the provided buffer", e);
            }
        }
        coder.encode(value, destBuf);
    }

    @Override
    public int getSerializedSize(T value) {
        int valLen = coder.getSerializedSize(value);
        if (coder.isLengthRequiredOnNestedContext()) {
            int lenLen = VarInt.getLength(valLen);
            return lenLen + valLen;
        } else {
            return valLen;
        }
    }

    @Override
    public T decode(ByteBuf data) {
        if (coder.isLengthRequiredOnNestedContext()) {
            int len;
            try {
                len = VarInt.decodeInt(data);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to decode length from the provided buffer", e);
            }
            int writerIndex = data.writerIndex();
            T value;
            try {
                data.writerIndex(data.readerIndex() + len);
                value = coder.decode(data);
            } finally {
                data.writerIndex(writerIndex);
            }
            return value;
        } else {
            return coder.decode(data);
        }
    }

    @Override
    public boolean isLengthRequiredOnNestedContext() {
        return false;
    }
}
