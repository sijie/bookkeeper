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

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;

/**
 * A reader that read entries from ledgers using synchronous api.
 */
public class SyncLedgerReader extends Thread implements AutoCloseable {

    private final ReadHandle readHandle;
    private final long endEntryId;
    private final int numEntriesPerRead;
    private volatile long nextEntryId;
    private volatile boolean running = false;

    public SyncLedgerReader(ReadHandle readHandle,
                            long startEntryId,
                            long endEntryId,
                            int numEntriesPerRead) {
        this.readHandle = readHandle;
        this.endEntryId = endEntryId;
        this.nextEntryId = startEntryId;
        this.numEntriesPerRead = numEntriesPerRead;
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
        try {
            // catchup reads: start reading from startEntryId till endEntryId or the current-known lastAddConfirmed
            // entry id
            catchingUp();
            if (endEntryId < 0) {
                // after catching up, the reader gets into a tailing read mode
                tailingRead();
            }
        } catch (Exception e) {
            System.err.println("Encountered exceptions on reading entries from ledger " + readHandle.getId());
            e.printStackTrace(System.err);
        }
    }

    // catch up reads from startEntryId
    void catchingUp() throws Exception {
        // determine what is the entry id to read up to. we can't read beyond lastAddConfirmed
        final long lastEntryId = this.endEntryId < 0
            ? readHandle.getLastAddConfirmed() : Math.min(this.endEntryId, readHandle.getLastAddConfirmed());

        System.out.println("Catching up on reading entries from " + nextEntryId + " to " + lastEntryId);
        readEntriesTill(lastEntryId);
    }

    void readEntriesTill(long lastEntryId) throws Exception {
        while (nextEntryId <= lastEntryId) {
            int numEntriesToRead = Math.min((int) (lastEntryId - nextEntryId + 1), numEntriesPerRead);

            // put the entries in a try block, so the entries are closed to release resources
            // once they are not used any more.
            try (LedgerEntries entries = readHandle.read(nextEntryId, nextEntryId + numEntriesToRead - 1)) {
                // process the entries read back from the read handle
                entries.forEach(entry -> System.out.println("Read entry : (eid = " + entry.getEntryId()
                    + ", " + new String(entry.getEntryBytes(), UTF_8) + ")"));
            }
            nextEntryId = nextEntryId + numEntriesToRead;
        }
    }

    void tailingRead() throws Exception {
        while (running) {
            // attempt to readLastAddConfirmed to retrieve the latest lastAddConfirmed
            readHandle.readLastAddConfirmed();
            if (nextEntryId > readHandle.getLastAddConfirmed()) {
                // we still don't see new entries appended. so we do a backoff and try to refresh lastAddConfirmed
                // after 100 ms
                TimeUnit.MILLISECONDS.sleep(100);
            } else {
                // the lastAddConfirmed is updated, try to read entries till the new lastAddConfirmed
                readEntriesTill(readHandle.getLastAddConfirmed());
            }
        }
    }

    @Override
    public void close() {
        running = false;
        interrupt();
    }
}
