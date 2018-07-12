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
import io.netty.buffer.ByteBufUtil;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * A byte array {@link Coder}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ByteArrayCoder implements Coder<byte[]> {

    private static final long serialVersionUID = 4394152634491445541L;

    public static ByteArrayCoder of() {
        return INSTANCE;
    }

    private static final ByteArrayCoder INSTANCE = new ByteArrayCoder();

    @Override
    public byte[] encode(byte[] value) {
        return value;
    }

    @Override
    public void encode(byte[] value, ByteBuf destBuf) {
        destBuf.writeBytes(value);
    }

    @Override
    public int getSerializedSize(byte[] value) {
        return value.length;
    }

    @Override
    public byte[] decode(ByteBuf data) {
        byte[] decodedData = ByteBufUtil.getBytes(data);
        data.readerIndex(data.writerIndex());
        return decodedData;
    }

    @Override
    public boolean isLengthRequiredOnNestedContext() {
        return true;
    }
}
