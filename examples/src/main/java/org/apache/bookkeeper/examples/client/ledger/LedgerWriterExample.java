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
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example to use how to append entries to a ledger using ledger api.
 */
public class LedgerWriterExample {

    private static final Logger log = LoggerFactory.getLogger(SyncLedgerWriter.class);

    private enum WriterType {
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
            names = { "-n", "--num-entries" },
            description = "num of entries to append")
        int numEntries;

        @Parameter(
            names = { "-r", "--rate" },
            description = "rate limiting at entries per second")
        double rate = 100;

        @Parameter(
            names = { "-w", "--writer-type" },
            description = "specify the type of api for this example to use. available : [ASYNC, SYNC]"
        )
        WriterType writerType = WriterType.SYNC;
    }

    public static void main(String[] args) {
        MainArgs mainArgs = new MainArgs();

        // parse the command line
        JCommander commander = new JCommander();
        commander.setProgramName("LedgerWriterExample");
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
            try (WriteHandle handle = bk.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("my-example".getBytes(UTF_8))
                .execute()
                .get()) {

                System.out.println("Successfully open ledger " + handle.getId() + " to append entries.");

                // once the ledger is open, start a writer thread that appends entries
                Thread writeThread;
                switch (mainArgs.writerType) {
                    case SYNC:
                        writeThread = new SyncLedgerWriter(handle, mainArgs.numEntries, mainArgs.rate);
                        break;
                    case ASYNC:
                        writeThread = new AsyncLedgerWriter(handle, mainArgs.numEntries, mainArgs.rate);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                            "WriterType '" + mainArgs.writerType + "' is not supported yet");
                }

                writeThread.start();
                writeThread.join();
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted at running ledger writer", e);
            Runtime.getRuntime().exit(0);
        } catch (IOException | BKException | ExecutionException e) {
            log.error("Encountered errors on running ledger writer", e);
            Runtime.getRuntime().exit(-1);
        }
    }

}
