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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.storage.SetupWriterResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.WriteEventSetResponse;
import org.apache.bookkeeper.stream.proto.storage.WriteRequest;
import org.apache.bookkeeper.stream.proto.storage.WriteResponse;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeWriter;
import org.apache.bookkeeper.stream.storage.api.stream.StreamStore;

/**
 * A write session that writes events into a given data range.
 */
@Slf4j
public class WriteSession
    implements StreamObserver<WriteRequest>, BiConsumer<WriteEventSetResponse, Throwable> {

    private final StreamStore rangeStore;
    private final StreamObserver<WriteResponse> respObserver;
    private final OrderedScheduler scheduler;
    private CompletableFuture<StreamRangeWriter> startFuture;
    private StreamRangeWriter rangeWriter;
    private ScheduledExecutorService executor;

    public WriteSession(StreamStore rangeStore,
                        StreamObserver<WriteResponse> respObserver,
                        OrderedScheduler scheduler) {
        this.rangeStore = rangeStore;
        this.respObserver = respObserver;
        this.scheduler = scheduler;
        if (log.isDebugEnabled()) {
            log.debug("Created write session ({})", respObserver);
        }
    }

    synchronized StreamRangeWriter getRangeWriter() {
        return rangeWriter;
    }

    @Override
    public void onNext(WriteRequest request) {
        switch (request.getReqCase()) {
            case SETUP_REQ:
                // set the executor for this write session
                executor = scheduler.chooseThread(request.getSetupReq().getStreamId());

                boolean failStart = false;
                CompletableFuture<StreamRangeWriter> future = null;
                synchronized (this) {
                    if (null != startFuture) {
                        log.warn("There is already a writer attempting to start writing in current session {}.", this);
                        failStart = true;
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Opening writer : {}", request.getSetupReq());
                        }
                        future = startFuture = rangeStore.openStreamRangeWriter(request.getSetupReq());
                    }
                }

                if (failStart) {
                    respObserver.onError(new StatusRuntimeException(Status.INTERNAL));
                }
                if (null != future) {
                    future.whenCompleteAsync((dataRangeWriter, throwable) -> {
                        onStartWriter(dataRangeWriter, throwable);
                    }, executor);
                }

                break;
            case WRITE_REQ:
                boolean failReq = false;
                StreamRangeWriter writer = null;
                synchronized (this) {
                    if (null == rangeWriter) {
                        log.warn("Received write requests before the writer is setup.");
                        failReq = true;
                    } else {
                        writer = rangeWriter;
                    }
                }
                if (failReq) {
                    respObserver.onError(new StatusRuntimeException(Status.INTERNAL));
                } else {
                    writer.write(request.getWriteReq()).whenCompleteAsync(this, executor);
                }
                break;
            default:
                log.warn("Received unknown write request {} : ", request);
                break;
        }
    }

    private void onStartWriter(StreamRangeWriter dataRangeWriter,
                               Throwable throwable) {
        if (null != throwable) {
            // exception on opening the data range writer
            respObserver.onError(new StatusRuntimeException(Status.INTERNAL));
            return;
        }
        synchronized (this) {
            this.rangeWriter = dataRangeWriter;
        }
        respObserver.onNext(WriteResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setSetupResp(SetupWriterResponse.newBuilder()
                .setWriterId(dataRangeWriter.getWriterId())
                .setLastEventSetId(dataRangeWriter.getLastEventSetId()))
            .build());
    }

    @Override
    public void accept(WriteEventSetResponse response,
                       Throwable throwable) {
        if (null != throwable) {
            log.error("Encountered exception on writing event set from session {}", respObserver, throwable);
            respObserver.onError(new StatusRuntimeException(Status.INTERNAL));
            onError(throwable);
            return;
        }
        respObserver.onNext(WriteResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setWriteResp(response)
            .build());
    }

    @Override
    public void onError(Throwable cause) {
        StreamRangeWriter rangeWriter = getRangeWriter();
        if (null != rangeWriter) {
            rangeWriter.onWriteSessionAborted(cause);
        }
    }

    @Override
    public void onCompleted() {
        StreamRangeWriter rangeWriter = getRangeWriter();
        if (null != rangeWriter) {
            rangeWriter.onWriteSessionCompleted();
        }
    }
}
