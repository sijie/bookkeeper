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

package org.apache.bookkeeper.examples.state;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.exceptions.LogEmptyException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example of how to use dlog api to replicate a set of values between machines.
 */
public class ReplicatedState<T> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedState.class);

    public interface SerDe<T> {
        /**
         * Serialize the given <tt>value</tt> into bytes.
         *
         * @param value value to serialize
         * @return the serialized bytes
         */
        byte[] serialize(T value);

        /**
         * Deserialize the value out of the given <tt>data</tt> bytes.
         *
         * @param data data bytes to deserialize
         * @return the deserialized value
         */
        T deserialize(byte[] data);
    }

    // read thread that replay log and apply updates to the in-memory state.
    private static class ReplayThread extends Thread {

        private final String logName;
        private final DistributedLogManager logManager;
        private final DLSN initialDLSN;
        // this is the barrier to tell the reader to replay log until this point
        // if it not set, the reader will keep tailing and replaying the log
        private final AtomicReference<DLSN> replayUtil;
        private final Consumer<LogRecord> logRecordProcessor;
        private final UncaughtExceptionHandler exceptionHandler;
        private volatile boolean running;
        private volatile DLSN lastReadDLSN = DLSN.InvalidDLSN;

        ReplayThread(DistributedLogManager logManager,
                     DLSN initialDLSN,
                     Consumer<LogRecord> logRecordProcessor,
                     UncaughtExceptionHandler exceptionHandler) {
            this.logManager = logManager;
            this.logName = logManager.getStreamName();
            this.initialDLSN = initialDLSN;
            this.replayUtil = new AtomicReference<>();
            this.logRecordProcessor = logRecordProcessor;
            this.exceptionHandler = exceptionHandler;
        }

        @Override
        public void run() {
            running = true;

            // open the log reader
            AsyncLogReader reader = null;
            while (running && null == reader) {
                try {
                    try {
                        reader = logManager.openAsyncLogReader(initialDLSN).get();
                        log.info("Successfully open log stream {}", logName);
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause();
                        if (cause instanceof LogEmptyException) {
                            // log might be not created yet
                            log.info("No records have been appended to log {} yet. retry in 1 second", logName);
                            TimeUnit.SECONDS.sleep(1);
                        } else if (cause instanceof LogNotFoundException) {
                            log.info("log {} is not created yet. retry in 1 second", logName);
                            TimeUnit.SECONDS.sleep(1);
                        } else {
                            log.error("Encountered exceptions on opening log {}", logName, cause);
                            exceptionHandler.uncaughtException(this, cause);
                            return;
                        }
                    }
                } catch (InterruptedException e) {
                    log.info("Interrupted at opening reader on log {}", logName, e);
                    continue;
                }
            }

            // replay the log stream
            try {
                while (running) {
                    LogRecordWithDLSN record;
                    try {
                        record = reader.readNext().get();
                        log.info("Read record {}", record);
                    } catch (InterruptedException e) {
                        log.info("Interrupted at replaying log {}", logName, e);
                        continue;
                    } catch (ExecutionException e) {
                        log.error("Encountered exception on replaying updates from log {}", logName, e.getCause());
                        exceptionHandler.uncaughtException(this, e.getCause());
                        return;
                    }
                    synchronized (this) {
                        DLSN dlsn = record.getDlsn();

                        // process the log record
                        logRecordProcessor.accept(record);
                        lastReadDLSN = dlsn;

                        // have reached end of the log stream to replay
                        if (replayUtil.get() != null && replayUtil.get().compareTo(dlsn) <= 0) {
                            return;
                        }
                    }
                }
            } finally {
                if (null != reader) {
                    reader.asyncClose();
                }
            }
        }

        public void terminateAtDLSN(DLSN endDLSN) {
            log.info("Terminating replay thread at {}", endDLSN);
            synchronized (this) {
                replayUtil.set(endDLSN);
                if (lastReadDLSN.compareTo(endDLSN) >= 0) {
                    terminate();
                }
            }
        }

        public void terminate() {
            running = false;
            interrupt();
        }

    }

    /**
     * The in-memory set to replicated between machines.
     */
    private final Set<T> memSet;
    /**
     * A serializer/deserializer to serialize value into bytes and deserialize bytes into values.
     */
    private final SerDe<T> serDe;

    // variables about dlog
    private final Namespace namespace;
    private final String logName;
    private DistributedLogManager logManager;

    // write thread
    private AsyncLogWriter writer;
    private long nextTxId;

    // read thread
    private ReplayThread replayThread;

    // a running latch, if any exception is thrown we simply shutdown the instance
    private final CompletableFuture<Void> runningLatch = new CompletableFuture<>();

    ReplicatedState(Namespace namespace,
                    String logName,
                    SerDe<T> serDe) {
        this.namespace = namespace;
        this.logName = logName;
        this.serDe = serDe;
        // initialize the state
        this.memSet = Collections.synchronizedSet(new HashSet<>());
        // start two threads, one is the write thread, which will attempt to lock the leadership
        // on the log; the other one is the read thread that tailing read from the stream and apply
        // the changes to the in-memory state.

    }

    public void start() throws IOException {
        log.info("Starting replicated set ...");

        logManager = namespace.openLog(logName);

        // start a replay thread to replay updates from the log
        replayThread = new ReplayThread(
            logManager,
            DLSN.InitialDLSN,
            (record) -> applyLogRecord(record),
            this::onExceptionCaught
        );
        replayThread.start();
        log.info("Successfully started a replay thread that replaying updates from log {}", logName);

        // attempt to acquire a lock to become the leader of the log
        AsyncLogWriter candidateWriter;
        try {
            candidateWriter = logManager.openAsyncLogWriter().get();
            LogRecord record;
            synchronized (this) {
                if (candidateWriter.getLastTxId() > 0) {
                    nextTxId = candidateWriter.getLastTxId() + 1;
                } else {
                    nextTxId = 0L;
                }
                // write a noop as a barrier to ensure the writer actually get the ownership
                Command command = Command.newBuilder()
                    .setNoOp(Noop.newBuilder().build())
                    .build();
                record = new LogRecord(nextTxId++, command.toByteArray());
            }
            DLSN latestDLSN = candidateWriter.write(record).get();
            log.info("Successfully write a barrier record to log {} at {}", logName, latestDLSN);

            // after I can successfully write a record to the log, I become the leader of this replicated set
            // terminate the replay at the DLSN I became the leader.
            replayThread.terminateAtDLSN(latestDLSN);
            // wait the replay thread to complete replay
            replayThread.join();
            replayThread = null;

            // set the candidate writer to writer, now it can take writes to this replicated set
            writer = candidateWriter;
            log.info("I am the leader for the replicated set (powered by log {}) at {}", logName, latestDLSN);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            onExceptionCaught(Thread.currentThread(), ie);
        } catch (ExecutionException ee) {
            onExceptionCaught(Thread.currentThread(), ee.getCause());
        }
    }

    @Override
    public void close() {
        runningLatch.complete(null);

        if (null != writer) {
            writer.asyncAbort();
        }

        if (null != replayThread) {
            replayThread.terminate();
            try {
                replayThread.join();
            } catch (InterruptedException e) {
                log.warn("Interrupted at waiting replay thread to quit", e);
            }
        }

        if (null != logManager) {
            try {
                logManager.close();
            } catch (IOException e) {
                log.warn("Failed to close log manager", e);
            }
        }
    }

    public void join() {
        try {
            runningLatch.get();
        } catch (InterruptedException e) {
            log.warn("Interrupted at running replicated set", e);
        } catch (ExecutionException e) {
            log.warn("Encountered exceptions at running replicated set", e.getCause());
        }
    }

    private void onExceptionCaught(Thread t, Throwable cause) {
        log.error("Encountered exception at running replicated set from thread {} :",
            t.getName(), cause);
        runningLatch.completeExceptionally(cause);
    }

    //
    // Reader to tail the log stream to rebuild the replicated set
    //

    private void applyLogRecord(LogRecord record) {
        Command command;
        try {
            command = Command.parseFrom(record.getPayload());
            T value;
            switch (command.getOpCase()) {
                case ADD_OP:
                    value = serDe.deserialize(command.getAddOp().getValue().toByteArray());
                    memSet.add(value);
                    System.out.println("add value : " + value);
                    break;
                case REMOVE_OP:
                    value = serDe.deserialize(command.getRemoveOp().getValue().toByteArray());
                    boolean removed = memSet.remove(value);
                    System.out.println("remove value '" + value + "' : " + removed);
                    break;
                case NO_OP:
                default:
                    // ignore these commands
                    break;
            }

        } catch (InvalidProtocolBufferException e) {
            log.error("Invalid log record to ");
        }
    }


    //
    // log all the updates to a log stream before applying them to the in-memory state
    //

    CompletableFuture<DLSN> logAddOp(T value) {
        Command command = Command.newBuilder()
            .setAddOp(AddValueOp.newBuilder()
                .setValue(ByteString.copyFrom(serDe.serialize(value)))
                .build())
            .build();
        return logCommand(command);
    }

    CompletableFuture<DLSN> logRemoveOp(T value) {
        Command command = Command.newBuilder()
            .setRemoveOp(RemoveValueOp.newBuilder()
                .setValue(ByteString.copyFrom(serDe.serialize(value)))
                .build())
            .build();
        return logCommand(command);
    }

    synchronized CompletableFuture<DLSN> logCommand(Command command) {
        if (null == writer) {
            CompletableFuture<DLSN> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException(
                "I am not the leader of the replicated state to mutate it"
            ));
            return future;
        }

        LogRecord record = new LogRecord(nextTxId++, command.toByteArray());
        return writer.write(record).whenComplete((dlsn, cause) -> {
            if (null != cause) {
                // encountered exceptions on appending updates to log.
                // we can be smart to by just giving up leadership and become follower
                // to just tail the log. but for the example, let's use fail-fast
                // mechanism for simplicity
                synchronized (ReplicatedState.class) {
                    // abort the writer
                    writer.asyncAbort();
                    writer = null;
                    // notify the replicated set exception is caught
                    onExceptionCaught(Thread.currentThread(), cause);
                }
            }
        });
    }

    //
    // Application interfaces to interact with the current
    //

    /**
     * Add a <tt>value</tt> to the replicated set.
     *
     * @param value the value to add to the replicated set.
     * @return a future represents the add result
     */
    CompletableFuture<Boolean> addValue(T value) {
        return logAddOp(value).thenApply(dlsn -> {
            // after the operation is logged successfully, applied to the in-memory state.
            return memSet.add(value);
        });
    }

    /**
     * Remove the <tt>value</tt> from the replicated set.
     *
     * @param value the value to remove from the replicated set
     * @return a future represents the remove result
     */
    CompletableFuture<Boolean> removeValue(T value) {
        return logRemoveOp(value).thenApply(dlsn -> {
            // after the operation is logged successfully, remove the value from the in-memory state.
            return memSet.remove(value);
        });
    }

    /**
     * Check if the value is in the replicated set.
     *
     * @param value the value to check if it exists in the replicated set.
     * @return true if the value exists, otherwise false.
     */
    boolean contains(T value) {
        return memSet.contains(value);
    }

}
