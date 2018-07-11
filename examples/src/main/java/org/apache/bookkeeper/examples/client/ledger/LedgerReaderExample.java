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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example to use how to read entries to a ledger using ledger api.
 */
public class LedgerReaderExample {

    private static final Logger log = LoggerFactory.getLogger(SyncLedgerWriter.class);

    private enum ReaderType {
        SYNC,
        ASYNC
    }

    private static class MainArgs {

        @Parameter(
            names = { "-u", "--service-uri"},
            required = true,
            description = "metadata service uri")
        String metadataServiceUri;

        @Parameter(
            names = { "-l", "--ledger-id" },
            required = true,
            description = "ledger id to read from")
        long ledgerEntryId = -1L;

        @Parameter(
            names = { "-si", "--start-entry-id" },
            description = "start entry id to read from")
        long startEntryId = 0L;

        @Parameter(
            names = { "-b", "--batch-size" },
            description = "num of entries to read per batch")
        int numEntriesPerRead = 1;

        @Parameter(
            names = { "-ei", "--end-entry-id" },
            description = "end entry id to read until (inclusive). -1 means keep tailing")
        long endEntryId = -1L;

        @Parameter(
            names = { "-r", "--reader-type" },
            description = "specify the type of api for this example to use. available : [ASYNC, SYNC]"
        )
        ReaderType readerType = ReaderType.SYNC;
    }

    public static void main(String[] args) {
        MainArgs mainArgs = new MainArgs();

        // parse the command line
        JCommander commander = new JCommander();
        commander.setProgramName("LedgerReaderExample");
        commander.addObject(mainArgs);
        try {
            commander.parse(args);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            commander.usage();
            Runtime.getRuntime().exit(-1);
            return;
        }

        // construct the bookkeeper client
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(mainArgs.metadataServiceUri);
        try (BookKeeper bk = BookKeeper.newBuilder(conf)
            .build()) {

            // create a ledger to append
            try (ReadHandle handle = bk.newOpenLedgerOp()
                .withDigestType(DigestType.CRC32C)
                .withLedgerId(mainArgs.ledgerEntryId)
                .withPassword("my-example".getBytes(UTF_8))
                .withRecovery(false)
                .execute()
                .get()) {

                System.out.println("Successfully open ledger " + handle.getId() + " to read entries.");

                // once the ledger is open, start a reader thread that reads entries
                Thread readThread;
                switch (mainArgs.readerType) {
                    case SYNC:
                        readThread = new SyncLedgerReader(
                            handle, mainArgs.startEntryId, mainArgs.endEntryId, mainArgs.numEntriesPerRead);
                        break;
                    case ASYNC:
                        readThread = new AsyncLedgerReader(
                            handle, mainArgs.startEntryId, mainArgs.endEntryId, mainArgs.numEntriesPerRead);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                            "ReaderType '" + mainArgs.readerType + "' is not supported yet");
                }

                readThread.start();
                readThread.join();
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted at running ledger reader", e);
            Runtime.getRuntime().exit(0);
        } catch (IOException | BKException | ExecutionException e) {
            log.error("Encountered errors on running ledger reader", e);
            Runtime.getRuntime().exit(-1);
        }
    }

}
