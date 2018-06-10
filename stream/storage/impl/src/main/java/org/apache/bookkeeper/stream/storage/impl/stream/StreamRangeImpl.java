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

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_WRITER_ID;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.RangePosition;
import org.apache.bookkeeper.stream.proto.storage.SetupReaderRequest;
import org.apache.bookkeeper.stream.proto.storage.SetupWriterRequest;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRange;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeReader;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeWriter;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.util.Utils;

/**
 * The default implementation of {@link StreamRange}. It is responsible for managing the writers and readers.
 */
@Slf4j
public class StreamRangeImpl implements StreamRange {

    private final long rid;
    private final DistributedLogManager manager;
    private CompletableFuture<AsyncLogWriter> openWriterFuture;
    private AsyncLogWriter logWriter;
    private OffsetSequencer offsetSequencer;

    public StreamRangeImpl(long rid,
                           DistributedLogManager manager) {
        this.rid = rid;
        this.manager = manager;
    }

    synchronized AsyncLogWriter getWriter() {
        return logWriter;
    }

    synchronized CompletableFuture<AsyncLogWriter> getWriterFuture() {
        return openWriterFuture;
    }

    @Override
    public CompletableFuture<RangePosition> getLastPosition() {
        CompletableFuture<RangePosition> future = FutureUtils.createFuture();
        manager.getLastLogRecordAsync().whenComplete(new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onSuccess(LogRecordWithDLSN record) {
                future.complete(RangePosition.newBuilder()
                    .setRangeId(rid)
                    .setOffset(record.getTransactionId())
                    .setSeqNum(DLSNUtils.getRangeSeqNum(record.getDlsn()))
                    .build());
            }

            @Override
            public void onFailure(Throwable cause) {
                future.completeExceptionally(cause);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<RangePosition> seal() {
        AsyncLogWriter writer;
        synchronized (this) {
            writer = logWriter;
        }
        if (writer != null) {
            return writer.markEndOfStream()
                .thenApply(offset ->
                    RangePosition.newBuilder()
                        .setRangeId(rid)
                        .setOffset(offset)
                        .setSeqNum(Long.MAX_VALUE)
                        .build());
        }
        CompletableFuture<StreamRangeWriter> future = FutureUtils.createFuture();
        doOpenWriter(INVALID_WRITER_ID, future);
        return future.thenCompose((ignored) -> seal());
    }

    @Override
    public CompletableFuture<StreamRangeWriter> openWriter(SetupWriterRequest request) {
        final CompletableFuture<StreamRangeWriter> future = FutureUtils.createFuture();

        // check if the writer is open or not.
        AsyncLogWriter writer;
        OffsetSequencer offsetSequencer;
        synchronized (this) {
            writer = logWriter;
            offsetSequencer = this.offsetSequencer;
        }
        if (null != writer) {
            future.complete(new StreamRangeWriterImpl(
                rid, INVALID_WRITER_ID, writer, offsetSequencer, StreamRangeImpl.this));
            return future;
        }
        doOpenWriter(INVALID_WRITER_ID, future);
        return future;
    }

    private void doOpenWriter(long writerId,
                              CompletableFuture<StreamRangeWriter> future) {
        // writer is not open, try to open the writer.
        CompletableFuture<AsyncLogWriter> openFuture;
        synchronized (this) {
            if (null != openWriterFuture) {
                openFuture = openWriterFuture;
            } else {
                openFuture = openWriterFuture = manager.openAsyncLogWriter();
            }
        }

        openFuture.whenComplete(new FutureEventListener<AsyncLogWriter>() {
            @Override
            public void onSuccess(AsyncLogWriter writer) {
                OffsetSequencer sequencer;
                synchronized (StreamRangeImpl.this) {
                    logWriter = writer;
                    offsetSequencer = new OffsetSequencer(writer.getLastTxId());
                    sequencer = offsetSequencer;
                }
                future.complete(new StreamRangeWriterImpl(
                    rid, writerId, writer, sequencer, StreamRangeImpl.this));
            }

            @Override
            public void onFailure(Throwable throwable) {
                // fail to open the writer, clear the future. so next retry will succeed
                synchronized (StreamRangeImpl.this) {
                    openWriterFuture = null;
                }
                future.completeExceptionally(throwable);
            }
        });
    }

    @Override
    public CompletableFuture<StreamRangeReader> openReader(SetupReaderRequest request) {
        final CompletableFuture<StreamRangeReader> future = FutureUtils.createFuture();
        DLSN fromDLSN = DLSNUtils.getDLSN(request.getRangeSeqNum());
        manager.openAsyncLogReader(fromDLSN).whenComplete(new FutureEventListener<AsyncLogReader>() {
            @Override
            public void onSuccess(AsyncLogReader reader) {
                future.complete(new StreamRangeReaderImpl(rid, reader));
            }

            @Override
            public void onFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    void abortAndResetLogWriter(AsyncLogWriter writerToAbort) {
        synchronized (this) {
            if (this.logWriter == writerToAbort) {
                this.logWriter = null;
                this.openWriterFuture = null;
            }
        }
        Utils.asyncClose(writerToAbort, true);
    }

    @Override
    public void close() {
        Utils.closeQuietly(getWriter());
        try {
            manager.close();
        } catch (IOException e) {
            log.warn("Encountered error on closing data range {}", rid, e);
        }
    }

}
