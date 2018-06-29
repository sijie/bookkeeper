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

import static org.apache.bookkeeper.tests.integration.stream.BkCtlCommandTester.BKCTL;
import static org.apache.bookkeeper.tests.integration.stream.BkCtlCommandTester.STREAM_STORAGE_URI_OPT;
import static org.apache.bookkeeper.tests.integration.stream.BkCtlCommandTester.TEST_STREAM;
import static org.apache.bookkeeper.tests.integration.stream.BkCtlCommandTester.TEST_TABLE;
import static org.junit.Assert.assertTrue;

import org.apache.bookkeeper.tests.containers.BKStandaloneContainer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.testcontainers.containers.Container.ExecResult;

/**
 * Run bkctl tests over a standalone cluster.
 */
public class BkCtlStandaloneTest {

    @Rule
    public final TestName testName = new TestName();

    @ClassRule
    public static BKStandaloneContainer bkContainer = new BKStandaloneContainer<>("integrationtest", 3);

    @BeforeClass
    public static void setupCluster() throws Exception {
        bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "namespace",
            "create",
            "default");
        ExecResult result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "tables",
            "create",
            TEST_TABLE);
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Successfully created table '" + TEST_TABLE + "'"));
        result = bkContainer.execCmd(
            BKCTL,
            STREAM_STORAGE_URI_OPT,
            "streams",
            "create",
            TEST_STREAM);
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Successfully created stream '" + TEST_STREAM + "'"));
    }

    private BkCtlCommandTester newTester() {
        return new BkCtlCommandTester(bkContainer);
    }

    //
    // `bookies` commands
    //

    @Test
    public void listBookies() throws Exception {
        newTester().listBookies();
    }

    //
    // `bookie` commands
    //

    @Test
    public void showLastMark() throws Exception {
        newTester().showLastMark();
    }

    //
    // `ledger` commands
    //

    @Test
    public void simpleTest() throws Exception {
        newTester().simpleTest(3);
    }

    //
    // `namespace` commands
    //

    @Test
    public void runNamespaceCommands() throws Exception {
        newTester().runNamespaceCommands(testName.getMethodName());
    }

    //
    // `tables` commands
    //

    @Test
    public void runTableCommands() throws Exception {
        newTester().runTableCommands(testName.getMethodName());
    }

    //
    // `table` commands
    //

    @Test
    public void putGetKey() throws Exception {
        newTester().putGetKey(testName.getMethodName());
    }

    @Test
    public void incGetKey() throws Exception {
        newTester().incGetKey(testName.getMethodName());
    }

    //
    // `streams` commands
    //

    @Test
    public void runStreamCommands() throws Exception {
        newTester().runStreamCommands(testName.getMethodName());
    }

    //
    // `stream` commands
    //

    @Test
    public void writeReadEvents() throws Exception {
        newTester().writeReadEvents(testName.getMethodName());
    }
}
