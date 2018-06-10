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

package org.apache.bookkeeper.stream.storage.impl.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.UnsafeByteOperations;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.storage.ReadEventSetResponse;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeReader;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;

/**
 * The default implementation of {@link StreamRangeReader}.
 */
class StreamRangeReaderImpl implements StreamRangeReader {

    private final long rid;
    private final AsyncLogReader reader;

    StreamRangeReaderImpl(long rid,
                          AsyncLogReader reader) {
        this.rid = rid;
        this.reader = reader;
    }

    @VisibleForTesting
    AsyncLogReader getReader() {
        return reader;
    }

    @Override
    public CompletableFuture<ReadEventSetResponse> readNext() {
        CompletableFuture<ReadEventSetResponse> readFuture = FutureUtils.createFuture();
        reader.readNext().whenComplete(new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onSuccess(LogRecordWithDLSN record) {
                readFuture.complete(ReadEventSetResponse.newBuilder()
                    .setData(UnsafeByteOperations.unsafeWrap(record.getPayloadBuf().nioBuffer()))
                    .setRangeId(rid)
                    .setRangeOffset(record.getTransactionId())
                    .setRangeSeqNum(DLSNUtils.getRangeSeqNum(record.getDlsn()))
                    .build());
            }

            @Override
            public void onFailure(Throwable throwable) {
                readFuture.completeExceptionally(throwable);
            }
        });
        return readFuture;
    }

    @Override
    public void onReadSessionAborted(Throwable cause) {
        reader.asyncClose();
    }

    @Override
    public void onReadSessionCompleted() {
        reader.asyncClose();
    }
}
