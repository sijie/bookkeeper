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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Ignore;
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
        baseConf.setPageLimit(1); // to make it easy to push ledger out of cache
        baseConf.setAutoRecoveryDaemonEnabled(true);
        baseConf.setBookieScannerEnabled(true);
    }

    @Ignore
    @Test
    public void testIndexFileMissing() throws Exception{
        // create an index file, assume the meta store has persist the ledger info
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32, "passwd".getBytes());
        long ledgerToDelete = lh.getId();
        long entryIds[] = new long[100];
        for (int i = 0; i < 100; i++) {
            entryIds[i] = lh.addEntry("testdata".getBytes());
        }
        lh.close();

        // push ledgerToCorrupt out of page cache (bookie is configured to only use 1 page)
        lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32, "passwd".getBytes());
        for (int i = 0; i < 100; i++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();

        //flush the index file to disk
        BookieAccessor.forceFlush(bs.get(0).getBookie());

        File ledgerDir = bsConfs.get(0).getLedgerDirs()[0];
        ledgerDir = Bookie.getCurrentDirectory(ledgerDir);
        // delete the index file
        File index = new File(ledgerDir, IndexPersistenceMgr.getLedgerName(ledgerToDelete));
        assertTrue(index.exists());
        LOG.info("file to delete{}" , index);
        assertTrue(index.delete());

        final CountDownLatch completeLatch = new CountDownLatch(100);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        BookieSocketAddress addr = Bookie.getBookieAddress(bsConfs.get(0));
        // access the ledger fail
        for (int i = 0; i < 100; i++) {
            bkc.getBookieClient().readEntry(addr, ledgerToDelete, entryIds[i],
                    new BookkeeperInternalCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc2, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
                    if (rc.compareAndSet(BKException.Code.OK, rc2)) {
                        LOG.info("Failed to read entry: {}:{}, reason: {}",
                                ledgerId, entryId, BKException.getMessage(rc2));
                    } else {
                        LOG.info("Failed to read entry : {} again", BKException.getMessage(rc2));
                    }
                    completeLatch.countDown();
                }
            }, addr);
        }
        completeLatch.await();
//        // using ledgerStorage to read data
//        for (int i = 0; i < 100; i++) {
//            getLedgerStorage(0).getEntry(ledgerToDelete, entryIds[i]);
//        }
        assertFalse("The ledger is expected under-replicated", rc.get() == BKException.Code.OK);

        // check the ledger is put into the suspicious list
        assertTrue(bookieScanner.isLedgerInSuspiciousList(ledgerToDelete));

        // trigger the fixing using URM
        bookieScanner.fixer.fixIndexFileMissing();

        // after fixed, access ledger again
        final CountDownLatch successLatch = new CountDownLatch(100);
        // access the ledger fail
        for (int i = 0; i < 100; i++) {
            bkc.getBookieClient().readEntry(bs.get(0).getLocalAddress(), ledgerToDelete, 1,
                    new BookkeeperInternalCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
                    assertTrue(rc == BKException.Code.OK);
                    successLatch.countDown();
                }
            }, bs.get(0).getLocalAddress());
        }
        successLatch.await();
    }

    /**
     * Test for fix corrupt index item plan b: scan entry log and update their locations.
     * @throws Exception
     */
    @Test
    public void testIndexFileCorruptPlanB() throws Exception{
        // get ledgerId to be corrupt
        long ledgerToCorrupt = prepareDataAndGetCorruptLedgerId();

        BookieAccessor.forceFlush(bs.get(0).getBookie());
        bookieScanner = getBookieScanner(0);

        File ledgerDir = bsConfs.get(0).getLedgerDirs()[0];
        ledgerDir = Bookie.getCurrentDirectory(ledgerDir);
        // corrupt of index file
        File index = new File(ledgerDir, IndexPersistenceMgr.getLedgerName(ledgerToCorrupt));
        LOG.info("file to corrupt{}" , index);
        ByteBuffer junk = ByteBuffer.allocate(1024 * 1024);
        FileOutputStream out = new FileOutputStream(index);
        out.getChannel().write(junk);
        out.close();

        final CountDownLatch completeLatch = new CountDownLatch(100);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        BookieSocketAddress addr = Bookie.getBookieAddress(bsConfs.get(0));
        // access the ledger fail, the entryId was started from 0
        for (int i = 0; i < 100; i++) {
            bkc.getBookieClient().readEntry(addr, ledgerToCorrupt, i,
                    new BookkeeperInternalCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc2, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
                    if (rc.compareAndSet(BKException.Code.OK, rc2)) {
                        LOG.info("Failed to read entry: {}:{}, reason: {}",
                                ledgerId, entryId, BKException.getMessage(rc2));
                    } else {
                        LOG.info("Failed to read entry : {}, there must has a read failure",
                                BKException.getMessage(rc2));
                    }
                    completeLatch.countDown();
                }
            }, addr);
        }
        completeLatch.await();
        assertFalse(rc.get() == BKException.Code.OK);

        // trigger the fixing by scan
        bookieScanner.fixer.fixCorruptIndexItemByScan();
        int [] rcs = new int[100];
        // after fixed, access ledger again
        final CountDownLatch successLatch = new CountDownLatch(100);
        // access the ledger success
        for (int i = 0; i < 100; i++) {
            bkc.getBookieClient().readEntry(bs.get(0).getLocalAddress(), ledgerToCorrupt, i,
                    new BookkeeperInternalCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
                    rcs[(int) entryId] = rc;
                    LOG.info("read entry: {}:{}, info: {}",
                                ledgerId, entryId, BKException.getMessage(rc));
                    successLatch.countDown();
                }
            }, bs.get(0).getLocalAddress());
        }
        successLatch.await();
        final IntStream successRcs = Arrays.stream(rcs);
        assertTrue(successRcs.sum() == BKException.Code.OK);
    }

    /**
     * Test for fix corrupt index item plan a: re-add entry and update their locations.
     * @throws Exception
     */
    @Test
    public void testIndexFileCorruptPlanA() throws Exception{
        // get ledgerId to be corrupt
        long ledgerToCorrupt = prepareDataAndGetCorruptLedgerId();

        BookieAccessor.forceFlush(bs.get(0).getBookie());
        bookieScanner = getBookieScanner(0);

        File ledgerDir = bsConfs.get(0).getLedgerDirs()[0];
        ledgerDir = Bookie.getCurrentDirectory(ledgerDir);
        // corrupt of index file
        File index = new File(ledgerDir, IndexPersistenceMgr.getLedgerName(ledgerToCorrupt));
        LOG.info("file to corrupt{}" , index);
        ByteBuffer junk = ByteBuffer.allocate(1024 * 1024);
        FileOutputStream out = new FileOutputStream(index);
        out.getChannel().write(junk);
        out.close();

        final CountDownLatch completeLatch = new CountDownLatch(100);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        BookieSocketAddress addr = Bookie.getBookieAddress(bsConfs.get(0));
        // access the ledger fail
        for (int i = 0; i < 100; i++) {
            bkc.getBookieClient().readEntry(addr, ledgerToCorrupt, i,
                new BookkeeperInternalCallbacks.ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(int rc2, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
                        if (rc.compareAndSet(BKException.Code.OK, rc2)) {
                            LOG.info("Failed to read entry: {}:{}, reason: {}",
                                    ledgerId, entryId, BKException.getMessage(rc2));
                        } else {
                            LOG.info("Failed to read entry : {}, there must has a read failure",
                                    BKException.getMessage(rc2));
                        }
                        completeLatch.countDown();
                    }
                }, addr);
        }
        completeLatch.await();
        assertFalse(rc.get() == BKException.Code.OK);

        // trigger the fixing by scan
        bookieScanner.fixer.fixCorruptIndexItemByReplay();

        // after fixed, access ledger again
        final CountDownLatch successLatch = new CountDownLatch(100);
        final AtomicInteger successRc = new AtomicInteger(BKException.Code.OK);
        // access the ledger success
        for (int i = 0; i < 100; i++) {
            bkc.getBookieClient().readEntry(bs.get(0).getLocalAddress(), ledgerToCorrupt, i,
                    new BookkeeperInternalCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
                            LOG.info("read entry: {}:{}, info: {}",
                                    ledgerId, entryId, BKException.getMessage(rc));
                            if (successRc.compareAndSet(BKException.Code.OK, rc)) {
                                LOG.info("Failed to read entry: {}:{}, reason: {}",
                                        ledgerId, entryId, BKException.getMessage(rc));
                            }
                            successLatch.countDown();
                        }
                    }, bs.get(0).getLocalAddress());
        }
        successLatch.await();
        assertTrue(successRc.get() == BKException.Code.OK);
    }

    private long prepareDataAndGetCorruptLedgerId() throws Exception{
        // create an index file, assume the meta store has persist the ledger info
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32, "passwd".getBytes());
        long ledgerToCorrupt = lh.getId();
        for (int i = 0; i < 100; i++) {
           lh.addEntry("testdata".getBytes());
        }
        lh.close();

        // push ledgerToCorrupt out of page cache (bookie is configured to only use 1 page)
        lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32, "passwd".getBytes());
        for (int i = 0; i < 100; i++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();
        return ledgerToCorrupt;
    }

    // todo create a test case where LedgerChecker can't find and the scanner can find
    // Mock Fixer to recover entry log
    @Test
    public void testEntryLogMissing(){
        bookieScanner = getBookieScanner(0);
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
    public void testEntryLogCorrupt(){
        bookieScanner = getBookieScanner(0);
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

    /**
     * get the scanner for the specific bs.
     * @return
     */
    private BookieScanner getBookieScanner(int index) {
        BookieServer server = bs.get(index);
        return ((InterleavedLedgerStorage) server.getBookie().ledgerStorage).scanner;
    }

    /**
     * get the LedgerStorage for the specific bs.
     * @return
     */
    private InterleavedLedgerStorage getLedgerStorage(int index) {
        BookieServer server = bs.get(index);
        return (InterleavedLedgerStorage) server.getBookie().ledgerStorage;
    }
}
