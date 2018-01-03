/**
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

package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for BookieScanner.
 */
public class BookieScannerTest extends BookKeeperClusterTestCase{

    private static final Logger LOG = LoggerFactory.getLogger(BookieScannerTest.class);
    BookieScanner bookieScanner;

    public BookieScannerTest() {
        super(3);
        bookieScanner = null;
    }
    @Before
    void setup(){
        baseConf.setAutoRecoveryDaemonEnabled(true);
    }
    @Test
    void testIndexFileMissing(){
        // create an index file, assume the meta store has persist the ledger info
        long ledgerId = 0;
        // delete the index file

        // access the ledger fail
//        assertFalse();

        // check the ledger is put into the suspicious list
        bookieScanner.isLedgerInSuspiciousList(ledgerId);
        // trigger the fixing
        bookieScanner.triggerFix();
        while (!bookieScanner.fixing.compareAndSet(false, true)){

        }
        // after fixed, access ledger again
//        assertTrue();

    }
    @Test
    void testIndexFileCorrupt(){
        // create an index file, assume the meta store has persist the ledger info
        long ledgerId = 0;
        // corrupt the index file

        // access the ledger's last entry fail
//        assertFalse();

        // check the ledger is put into the suspicious list
        bookieScanner.isLedgerInSuspiciousList(ledgerId);
        // trigger the fixing
        bookieScanner.triggerFix();
        while (!bookieScanner.fixing.compareAndSet(false, true)){

        }
        // after fixed, access ledger again
//        assertTrue();

    }

    // Mock Fixer to recover entry log
    @Test
    void testEntryLogMissing(){
        // create an entry log, and record entry offset info
        long entryLogId = 0;
        // delete the entry log

        // access the entrylog's entry fail
//        assertFalse();

        // check the ledger is put into the suspicious list
        bookieScanner.isEntryLogInSuspiciousList(entryLogId);
        // trigger the fixing
        bookieScanner.triggerFix();
        while (!bookieScanner.fixing.compareAndSet(false, true)){

        }
        // after fixed, access entry again
//        assertTrue();
    }

    @Test
    void testEntryLogCorrupt(){
        // create an entry log, and record entry offset info
        long entryLogId = 0;
        // delete the entry log

        // access the entrylog's entry fail
//        assertFalse();

        // check the ledger is put into the suspicious list
        bookieScanner.isEntryLogInSuspiciousList(entryLogId);
        // trigger the fixing
        bookieScanner.triggerFix();
        while (!bookieScanner.fixing.compareAndSet(false, true)){

        }
        // after fixed, access entry again
//        assertTrue();
    }
}
