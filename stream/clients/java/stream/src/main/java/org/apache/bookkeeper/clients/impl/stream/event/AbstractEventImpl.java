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

import io.netty.util.ReferenceCountUtil;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Setter;
import org.apache.bookkeeper.api.stream.Event;

/**
 * Abstract event implementation.
 */
@Setter(AccessLevel.PACKAGE)
abstract class AbstractEventImpl<KeyT, ValueT> implements Event<KeyT, ValueT> {

    protected KeyT key;
    protected ValueT value;
    protected long timestamp;

    protected void reset() {
        ReferenceCountUtil.safeRelease(key);
        key = null;
        ReferenceCountUtil.safeRelease(value);
        value = null;
        timestamp = -1L;
    }

    @Nullable
    @Override
    public KeyT key() {
        return key;
    }

    @Override
    public ValueT value() {
        return value;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

}
