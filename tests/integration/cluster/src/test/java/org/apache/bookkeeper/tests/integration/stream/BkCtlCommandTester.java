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

package org.apache.bookkeeper.tests.integration.stream;

import static org.junit.Assert.assertTrue;

import org.apache.bookkeeper.tests.containers.ChaosContainer;
import org.testcontainers.containers.Container.ExecResult;

/**
 * A bkctl command tester.
 */
public class BkCtlCommandTester {

    protected static final String BKCTL = "/opt/bookkeeper/bin/bkctl";
    protected static final String STREAM_STORAGE_URI_OPT = "--service-uri bk://localhost:4181";
    protected static final String TEST_TABLE = "test-table";
    protected static final String TEST_STREAM = "test-stream";

    private final ChaosContainer bkContainer;

    BkCtlCommandTester(ChaosContainer container) {
        this.bkContainer = container;
    }

    //
    // `bookies` commands
    //

    public void listBookies() throws Exception {
        ExecResult result = bkContainer.execCmd(
            BKCTL,
            "bookies",
            "list"
        );
        String output = result.getStdout();
        assertTrue(output.contains("ReadWrite Bookies :"));
    }

    //
    // `bookie` commands
    //

    public void showLastMark() throws Exception {
        ExecResult result = bkContainer.execCmd(
            BKCTL,
            "bookie",
            "lastmark"
        );
        assertTrue(result.getStdout().contains("LastLogMark : Journal"));
    }

    //
    // `ledger` commands
    //

    public void simpleTest(int numBookies) throws Exception {
        ExecResult result = bkContainer.execCmd(
            BKCTL,
            "ledger",
            "simpletest",
            "--ensemble-size", "" + numBookies,
            "--write-quorum-size", "" + numBookies,
            "--ack-quorum-size", "" + (numBookies - 1),
            "--num-entries", "100"
        );
        assertTrue(
            result.getStdout().contains("100 entries written to ledger"));
    }

    //
    // `namespace` commands
    //

    public void runNamespaceCommands(String nsName) throws Exception {
        ExecResult result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "namespace",
            "create",
            nsName
        );
        assertTrue(
            result.getStdout().contains("Successfully created namespace '" + nsName + "'"));
    }

    //
    // `tables` commands
    //

    public void runTableCommands(String tableName) throws Exception {
        ExecResult result;

        // get a not-found table
        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "tables",
            "get",
            tableName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Table '" + tableName + "' is not found"));

        // delete a not-found table
        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "tables",
            "delete",
            tableName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Table '" + tableName + "' is not found"));

        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "tables",
            "create",
            tableName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Successfully created table '" + tableName + "'"));

        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "tables",
            "get",
            tableName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("stream_name: \"" + tableName + "\""));

        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "tables",
            "delete",
            tableName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Successfully deleted table '" + tableName + "'"));

        // check table is deleted
        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "tables",
            "get",
            tableName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Table '" + tableName + "' is not found"));
    }

    //
    // `table` commands
    //

    public void putGetKey(String prefix) throws Exception {
        String key = prefix + "-key";
        String value = prefix + "-value";
        ExecResult result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "table",
            "put",
            TEST_TABLE,
            key,
            value
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains(String.format("Successfully update kv: ('%s', '%s')",
                key, value
                )));

        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "table",
            "get",
            TEST_TABLE,
            key);
        assertTrue(
            result.getStdout().contains(String.format("value = %s", value)));
    }

    public void incGetKey(String prefix) throws Exception {
        String key = prefix + "-key";
        long value = System.currentTimeMillis();
        ExecResult result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "table",
            "inc",
            TEST_TABLE,
            key,
            "" + value
        );
        assertTrue(
            result.getStdout().contains(String.format("Successfully increment kv: ('%s', amount = '%s').",
                key, value
                )));

        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "table",
            "get",
            TEST_TABLE,
            key);
        assertTrue(
            result.getStdout().contains(String.format("value = %s", value)));
    }

    //
    // `streams` commands
    //

    public void runStreamCommands(String streamName) throws Exception {
        ExecResult result;

        // get a not-found stream
        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "streams",
            "get",
            streamName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Stream '" + streamName + "' is not found"));

        // delete a not-found stream
        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "streams",
            "delete",
            streamName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Stream '" + streamName + "' is not found"));

        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "streams",
            "create",
            streamName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Successfully created stream '" + streamName + "'"));

        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "streams",
            "get",
            streamName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("stream_name: \"" + streamName + "\""));

        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "streams",
            "delete",
            streamName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Successfully deleted stream '" + streamName + "'"));

        // check stream is deleted
        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "streams",
            "get",
            streamName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Stream '" + streamName + "' is not found"));
    }

    //
    // `stream` commands
    //

    public void writeReadEvents(String prefix) throws Exception {
        final int numEvents = 2;
        final String key = prefix + "-key";
        final String streamName = prefix + "-stream";

        ExecResult result;
        // create a test stream
        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "streams",
            "create",
            streamName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Successfully created stream '" + streamName + "'"));

        // write 10 events
        for (int i = 0; i < numEvents; i++) {
            result = bkContainer.execCmd(
                BKCTL,
                STREAM_STORAGE_URI_OPT,
                "stream",
                "write",
                streamName,
                "-k", key,
                "-t " + i,
                prefix + "-" + i);
            assertTrue(
                result.getStdout(),
                result.getStdout().contains("Successfully write event at position"));
        }

        // read 10 events
        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "stream",
            "read",
            streamName,
            "-n " + numEvents);
        for (int i = 0; i < numEvents; i++) {
            assertTrue(
                result.getStdout(),
                result.getStdout().contains(String.format("key = '%s', timestamp = %d", key, i)));
            assertTrue(
                result.getStdout(),
                result.getStdout().contains(prefix + "-" + i));
        }
    }

}
