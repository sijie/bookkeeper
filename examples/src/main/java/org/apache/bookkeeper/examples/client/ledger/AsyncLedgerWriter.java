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

import com.google.common.util.concurrent.RateLimiter;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A writer that appends entries into ledgers using async api.
 */
public class AsyncLedgerWriter extends Thread implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AsyncLedgerWriter.class);

    private final WriteHandle writeHandle;
    private final int numEntries;
    private final double rate;
    private volatile boolean running = false;

    public AsyncLedgerWriter(WriteHandle handle,
                             int numEntries,
                             double rate) {
        this.writeHandle = handle;
        this.numEntries = numEntries;
        this.rate = rate;
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        super.start();
    }

    @Override
    public void close() {
        running = false;
        interrupt();
    }

    @Override
    public void run() {
        int numWritten = 0;
        RateLimiter limiter = RateLimiter.create(rate);
        Mutable<Throwable> writeException = new MutableObject<>();
        while (running
            && (numEntries <= 0 || numWritten < numEntries)) {
            if (writeException.getValue() != null) {
                System.err.println("Aborted ledger writer due to encountering exception");
                return;
            }
            limiter.acquire();
            String entryStr = "async-entry-" + numWritten;

            writeHandle.appendAsync(entryStr.getBytes(UTF_8))
                .thenApply(entryId -> {
                    System.out.println("Append entry : (eid = " + entryId + ", " + entryStr + ")");
                    return null;
                })
                .exceptionally(ex -> {
                    System.err.println("Failed to append entry '" + entryStr + "' : ");
                    ex.printStackTrace(System.err);
                    return null;
                });

            // append entries
            ++numWritten;
        }
    }

}
