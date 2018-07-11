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

package org.apache.bookkeeper.examples.client.ledger;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader that read entries from ledgers using async api.
 */
public class AsyncLedgerReader extends Thread implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AsyncLedgerReader.class);

    private final ReadHandle readHandle;
    private final long endEntryId;
    private final int numEntriesPerRead;
    private final AtomicLong nextEntryId;
    private volatile boolean running = false;
    private final CompletableFuture<Void> runningFuture = new CompletableFuture<>();
    private final AtomicReference<Throwable> readError = new AtomicReference<>();
    private final ScheduledExecutorService readExecutor;

    public AsyncLedgerReader(ReadHandle readHandle,
                             long startEntryId,
                             long endEntryId,
                             int numEntriesPerRead) {
        this.readHandle = readHandle;
        this.endEntryId = endEntryId;
        this.nextEntryId = new AtomicLong(startEntryId);
        this.numEntriesPerRead = numEntriesPerRead;
        this.readExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        super.start();
    }

    @Override
    public void run() {
        catchingUp()
            .thenAccept(lastEntryId -> {
                System.out.println("Successfully caught up at entry " + lastEntryId);
                if (endEntryId < 0) {
                    tailingRead();
                } else {
                    // we have successfully finished reading all entries, so complete the thread
                    runningFuture.complete(null);
                }
            })
            .exceptionally(cause -> {
                runningFuture.completeExceptionally(cause);
                return null;
            });

        try {
            runningFuture.get();
        } catch (InterruptedException e) {
            log.info("Interrupted at reading entries from ledger " + readHandle.getId());
        } catch (ExecutionException e) {
            System.err.println("Encountered exceptions on reading entries from ledger " + readHandle.getId());
            e.getCause().printStackTrace(System.err);
        }
    }

    // catch up reads from startEntryId
    CompletableFuture<Long> catchingUp() {
        // determine what is the entry id to read up to. we can't read beyond lastAddConfirmed
        final long lastEntryId = this.endEntryId < 0
            ? readHandle.getLastAddConfirmed() : Math.min(this.endEntryId, readHandle.getLastAddConfirmed());

        System.out.println("Catching up on reading entries from " + nextEntryId + " to " + lastEntryId);
        return readEntriesTill(lastEntryId);
    }

    CompletableFuture<Long> readEntriesTill(final long lastEntryId) {
        CompletableFuture<Long> readCompleteFuture = new CompletableFuture<>();
        readBatchTill(lastEntryId, readCompleteFuture);
        return readCompleteFuture;
    }

    void readBatchTill(final long lastEntryId, CompletableFuture<Long> readCompleteFuture) {
        if (nextEntryId.get() > lastEntryId) {
            // we have caught up to lastEntryId, satisfy the `readCompleteFuture`
            readCompleteFuture.complete(lastEntryId);
            return;
        } else {
            final long beginEntryId = nextEntryId.get();
            int numEntriesToRead = Math.min((int) (lastEntryId - beginEntryId + 1), numEntriesPerRead);

            readHandle.readAsync(beginEntryId, beginEntryId + numEntriesToRead - 1)
                .thenAccept(entries -> {
                    // process the entries read back from the read handle
                    try {
                        entries.forEach(entry -> System.out.println("Read entry : (eid = " + entry.getEntryId()
                            + ", " + new String(entry.getEntryBytes(), UTF_8) + ")"));
                        // read the next batch
                        nextEntryId.addAndGet(numEntriesToRead);
                        readBatchTill(lastEntryId, readCompleteFuture);
                    } finally {
                        entries.close();
                    }
                })
                .exceptionally(cause -> {
                    readError.set(cause);
                    readCompleteFuture.completeExceptionally(cause);
                    return null;
                });
        }
    }

    void tailingRead() {
        if (!running) {
            runningFuture.complete(null);
            return;
        } else {
            readHandle.readLastAddConfirmedAsync()
                .thenCompose(lac -> {
                    if (nextEntryId.get() > lac) {
                        // we still don't see new entries appended. do a backoff and try to refresh lastAddConfirmed
                        // after 100ms
                        readExecutor.schedule(() -> tailingRead(), 100, TimeUnit.MILLISECONDS);
                        return FutureUtils.value(lac);
                    } else {
                        // the lastAddConfirmed is updated, try to read entries till the new lastAddConfirmed
                        return readEntriesTill(lac)
                            .thenApplyAsync(lastEntryId -> {
                                tailingRead();
                                return lastEntryId;
                            }, readExecutor);
                    }
                })
                .exceptionally(cause -> {
                    readError.set(cause);
                    runningFuture.completeExceptionally(cause);
                    return null;
                });
        }
    }

    @Override
    public void close() {
        running = false;
        runningFuture.complete(null);
        readExecutor.shutdown();
    }
}
