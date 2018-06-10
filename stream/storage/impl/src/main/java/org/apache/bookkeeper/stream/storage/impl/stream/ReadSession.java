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
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stream.proto.storage.ReadEventSetResponse;
import org.apache.bookkeeper.stream.proto.storage.ReadRequest;
import org.apache.bookkeeper.stream.proto.storage.ReadResponse;
import org.apache.bookkeeper.stream.proto.storage.SetupReaderResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.storage.api.stream.StreamRangeReader;
import org.apache.bookkeeper.stream.storage.api.stream.StreamStore;
import org.apache.distributedlog.exceptions.EndOfStreamException;

/**
 * A write session that writes events into a given data range.
 */
@Slf4j
public class ReadSession implements StreamObserver<ReadRequest> {

    private final StreamStore rangeStore;
    private final StreamObserver<ReadResponse> respObserver;
    private CompletableFuture<StreamRangeReader> startFuture;
    private StreamRangeReader rangeReader;
    private CompletableFuture<ReadEventSetResponse> outstandingRead = null;
    private long requestedSize = 0L;
    private long deliveredSize = 0L;
    private boolean closed = false;
    private int readSessionId = -1;

    public ReadSession(StreamStore rangeStore,
                       StreamObserver<ReadResponse> respObserver) {
        this.rangeStore = rangeStore;
        this.respObserver = respObserver;
        if (log.isDebugEnabled()) {
            log.debug("Created read session ({})", respObserver);
        }
    }

    synchronized StreamRangeReader getRangeReader() {
        return rangeReader;
    }

    @Override
    public void onNext(ReadRequest request) {
        switch (request.getReqCase()) {
            case SETUP_REQ:
                boolean failStart = false;
                CompletableFuture<StreamRangeReader> future = null;
                synchronized (this) {
                    if (null != startFuture) {
                        log.warn("There is already a reader attempting to start reading in current session {}.", this);
                        failStart = true;
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Opening reader : {}", request.getSetupReq());
                        }
                        future = startFuture = rangeStore.openStreamRangeReader(request.getSetupReq());
                    }
                }
                if (failStart) {
                    onError(new StatusRuntimeException(Status.INTERNAL));
                }
                if (null != future) {
                    future.whenComplete((dataRangeReader, throwable) -> {
                        if (null != throwable) {
                            // exception on opening the data range writer
                            onError(new StatusRuntimeException(Status.INTERNAL));
                            return;
                        }
                        synchronized (this) {
                            this.rangeReader = dataRangeReader;
                        }
                        readSessionId = request.getSetupReq().getReadSessionId();
                        respObserver.onNext(ReadResponse.newBuilder()
                            .setCode(StatusCode.SUCCESS)
                            .setReadSessionId(readSessionId)
                            .setSetupResp(SetupReaderResponse.newBuilder().build())
                            .build());
                    });
                }
                break;
            case READ_REQ:
                boolean failReq = false;
                synchronized (this) {
                    if (null == rangeReader) {
                        log.warn("Received read requests before the reader is setup.");
                        failReq = true;
                    }
                }
                if (failReq) {
                    onError(new StatusRuntimeException(Status.INTERNAL));
                } else {
                    requestMoreBytes(request.getReadReq().getMaxNumBytes());
                }
                break;
            default:
                log.warn("Received unknown read request : {}", request);
                break;
        }
    }

    synchronized void requestMoreBytes(long maxNumBytes) {
        if (closed) {
            return;
        }

        boolean readNext = deliveredSize >= requestedSize;
        requestedSize += maxNumBytes;
        readNext = readNext & (requestedSize > deliveredSize);
        if (!readNext) {
            return;
        }
        readNextEventSet();
    }

    synchronized void readNextEventSet() {
        // read next
        CompletableFuture<ReadEventSetResponse> readFuture = null;
        if (null == outstandingRead) {
            readFuture = outstandingRead = rangeReader.readNext();
        }
        if (null != readFuture) {
            readFuture.whenComplete((readEventSetResponse, throwable) -> {
                synchronized (this) {
                    outstandingRead = null;
                }

                if (null == throwable) {
                    deliverEventSet(readEventSetResponse);
                    return;
                }
                handleReadException(throwable);
            });
        }
    }

    void deliverEventSet(ReadEventSetResponse response) {
        respObserver.onNext(ReadResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setReadSessionId(readSessionId)
            .setReadResp(response)
            .build());
        synchronized (this) {
            deliveredSize += response.getData().size();
            if (deliveredSize >= requestedSize) {
                // do nothing
                return;
            }
            // we haven't delivered enough bytes that the reader requests
            readNextEventSet();
        }
    }

    void handleReadException(Throwable cause) {
        if (cause instanceof EndOfStreamException) {
            respObserver.onNext(ReadResponse.newBuilder()
                .setCode(StatusCode.END_OF_STREAM_RANGE)
                .setReadSessionId(readSessionId)
                .build());
            respObserver.onCompleted();
            return;
        }
        // closing the reader
        onError(cause);
    }

    @Override
    public void onError(Throwable cause) {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }

        respObserver.onError(new StatusRuntimeException(Status.INTERNAL));

        StreamRangeReader rangeReader = getRangeReader();
        if (null != rangeReader) {
            rangeReader.onReadSessionAborted(cause);
        }
    }

    @Override
    public void onCompleted() {
        synchronized (this) {
            closed = true;
        }

        StreamRangeReader rangeReader = getRangeReader();
        if (null != rangeReader) {
            rangeReader.onReadSessionCompleted();
        }
    }
}
