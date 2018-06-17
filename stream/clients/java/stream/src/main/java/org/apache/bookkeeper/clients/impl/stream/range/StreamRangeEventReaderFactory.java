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

import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.clients.impl.stream.event.ReadEventsImpl;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * Factory to create {@link StreamRangeEventReader}.
 */
public interface StreamRangeEventReaderFactory {

    /**
     * Create a range event reader.
     *
     * @param streamConfig stream config
     * @param streamProps stream properties
     * @param rangeProps range properties
     * @param rangePos range position
     * @param readEventsRecycler read events instance recycler
     * @return a range event reader.
     */
    <KeyT, ValueT> StreamRangeEventReader<KeyT, ValueT> createRangeEventReader(
        StreamConfig<KeyT, ValueT> streamConfig,
        StreamProperties streamProps,
        RangeProperties rangeProps,
        RangePosition rangePos,
        ReadEventsImpl.Recycler<KeyT, ValueT> readEventsRecycler);

}
