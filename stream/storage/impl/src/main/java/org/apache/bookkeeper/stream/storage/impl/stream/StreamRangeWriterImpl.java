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
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetRequest;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetResponse;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeWriter;
import org.apache.bookkeeper.stream.storage.exceptions.ConditionalWriteException;
import org.apache.bookkeeper.stream.storage.exceptions.UnimplementedException;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;

/**
 * The default implementation of {@link StreamRangeWriter}.
 */
class StreamRangeWriterImpl implements StreamRangeWriter {

    private final long rangeId;
    private final long writerId;
    private final AsyncLogWriter writer;
    private final OffsetSequencer sequencer;
    private final StreamRangeImpl rangeImpl;

    StreamRangeWriterImpl(long rangeId,
                          long writerId,
                          AsyncLogWriter writer,
                          OffsetSequencer sequencer,
                          StreamRangeImpl rangeImpl) {
        this.rangeId = rangeId;
        this.writerId = writerId;
        this.writer = writer;
        this.sequencer = sequencer;
        this.rangeImpl = rangeImpl;
    }

    @VisibleForTesting
    AsyncLogWriter getWriter() {
        return writer;
    }

    @Override
    public long getWriterId() {
        return writerId;
    }

    @Override
    public long getLastEventSetId() {
        return -1L;
    }

    @Override
    public CompletableFuture<WriteEventSetResponse> write(WriteEventSetRequest request) {
        CompletableFuture<DLSN> dlogWriteFuture;
        long offset;
        synchronized (sequencer) {
            switch (request.getExpectedPositionCase()) {
                case EXPECTEDPOSITION_NOT_SET:
                    break;
                case EXPECTED_RANGE_OFFSET:
                    if (request.getExpectedRangeId() != rangeId
                        || request.getExpectedRangeOffset() != sequencer.currentOffset()) {
                        String errorMsg = String.format(
                            "Conditional write failure on stream range '%s': expected - (%s@%s), actual - (%s@%s)",
                            writer.getStreamName(),
                            sequencer.currentOffset(), rangeId,
                            request.getExpectedRangeOffset(), request.getExpectedRangeId());
                        return FutureUtils.exception(new ConditionalWriteException(errorMsg));
                    }
                    break;
                default:
                    return FutureUtils.exception(new UnimplementedException(
                        "Conditional write with sequence number is not implemented"));
            }

            offset = sequencer.currentOffset();
            LogRecord record = new LogRecord(offset, request.getData().asReadOnlyByteBuffer());
            dlogWriteFuture = writer.write(record);
            sequencer.advanceOffset(request.getData().size());
        }

        CompletableFuture<WriteEventSetResponse> future = FutureUtils.createFuture();
        dlogWriteFuture.whenComplete(new FutureEventListener<DLSN>() {
            @Override
            public void onSuccess(DLSN dlsn) {
                WriteEventSetResponse resp = WriteEventSetResponse.newBuilder()
                    .setEventSetId(request.getEventSetId())
                    .setRangeId(rangeId)
                    .setRangeOffset(offset)
                    .setRangeSeqNum(DLSNUtils.getRangeSeqNum(dlsn))
                    .build();
                future.complete(resp);
            }

            @Override
            public void onFailure(Throwable throwable) {
                rangeImpl.abortAndResetLogWriter(writer);
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    @Override
    public void onWriteSessionAborted(Throwable cause) {
        // the writer is shared across multiple write sessions. so when a session is aborted,
        // we don't close the underlying writer directly.
    }

    @Override
    public void onWriteSessionCompleted() {
        // the writer is shared across multiple write sessions. so when a session is completed,
        // we don't close the underlying writer directly.
    }
}
