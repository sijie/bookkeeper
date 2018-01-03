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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.collections.ConcurrentOpenHashSet;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for scanning ledger index files and entry logs to keep data's integrity.
 */
public class BookieScanner {
    private static final Logger LOG = LoggerFactory.getLogger(BookieScanner.class);

    private final ServerConfiguration conf;
    private final EntryLogger entryLogger;
    final CompactableLedgerStorage ledgerStorage;
    final LedgerCache ledgerCache;
    final LedgerManager ledgerManager;
    private final BookieSocketAddress selfBookieAddress;

    // includes ledgers whose index file is missing
    private ConcurrentOpenHashSet<Long> suspiciousLedgers;
    // contains corrupt index from offset to entry: entryKeyOffset -/-> entry
    private ConcurrentHashMap<Long, Set<Long>> corruptIndexLedgers;
    private ConcurrentOpenHashSet<Pair<Long, Long>> corruptEntryIndices;
    // corrupt entries in entryLog
    private ConcurrentOpenHashSet<Pair<Long, Long>> corruptEntries;
    //todo use what structures to store ledgers (heap?) and cursor ?
    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, LedgerFragment>> corruptFragments;
    private ConcurrentOpenHashSet<Long> suspiciousEntryLogs;
    private ConcurrentOpenHashSet<Long> corruptLedgers;
    private ConcurrentOpenHashSet<Long> corruptEntryLogs;
    private int scanCursor;
    // parameter for scanning, should pass to GCThread to control the scan frequency.
    private int scanInterval;
    private int persistentInterval;

    // flag to indicate fix is in progress
    final AtomicBoolean fixing = new AtomicBoolean(false);
    private final Fixer fixer;
    // Expose Stats
    private final StatsLogger statsLogger;

    public BookieScanner(ServerConfiguration conf, CompactableLedgerStorage ledgerStorage, LedgerManager ledgerManager,
                         EntryLogger entryLogger, LedgerCache ledgerCache, StatsLogger statsLogger) throws IOException {
        this.conf = conf;
        this.entryLogger = entryLogger;
        this.statsLogger = statsLogger;
        this.ledgerStorage = ledgerStorage;
        this.ledgerCache = ledgerCache;
        this.ledgerManager = ledgerManager;
        this.selfBookieAddress = Bookie.getBookieAddress(conf);
        try {
            this.fixer = new Fixer();
        } catch (Exception e) {
            LOG.warn("Facing exception {} when constructing Fixer at BookieScanner.", e);
            throw new IOException("Failed to instantiate Fixer at BookieScanner", e);
        }
    }

    /**
     * Scan Index dirs to find suspicious ledger: find those should in local but not.
     * Check ledger in (set(ledgersInMeta) - set(ledgersInLocal)) whether
     * use this bk as part of ensemble.
     */
    private void scanIndexDirs() throws IOException {
        Set<Long> ledgersToCheck = new TreeSet<Long>();
        try {
            // Get a set of all ledgers on the bookie
            NavigableSet<Long> bkActiveLedgers = Sets.newTreeSet(((LedgerCacheImpl) ledgerCache).scanIndexDirs());

            // Iterate over all the ledger on the metadata store
            LedgerManager.LedgerRangeIterator ledgerRangeIterator = ledgerManager.getLedgerRanges();

            if (!ledgerRangeIterator.hasNext()) {
                // Empty global active ledgers, no need to go on.
               return;
            }
            while (ledgerRangeIterator.hasNext()) {
                LedgerManager.LedgerRange lRange = ledgerRangeIterator.next();
                Set<Long> ledgersInMetadata = lRange.getLedgers();
                for (Long metaStoreLid : ledgersInMetadata) {
                    if (!bkActiveLedgers.contains(metaStoreLid)) {
                        ledgersToCheck.add(metaStoreLid);
                    }
                }
            }
        } catch (Throwable t) {
            // ignore exception, check next time
            LOG.warn("Exception when iterating over the metadata {}", t);
        }
        // check ledger's ensemble whether contains local bk
        ledgersToCheck.forEach(ledgerId -> {
            if (isSuspiciousLedger(ledgerId)) {
                addToSuspiciousLedgers(ledgerId);
            }
        });
    }

    /**
     * Check whether the ledger is suspicious.
     */
    private boolean isSuspiciousLedger(long ledgerId) {
        final AtomicBoolean result = new AtomicBoolean(false);
        ledgerManager.readLedgerMetadata(ledgerId, new BookkeeperInternalCallbacks.GenericCallback<LedgerMetadata>() {
            @Override
            public void operationComplete(int rc, LedgerMetadata ledgerMetadata) {
                if (rc == BKException.Code.OK) {
                    SortedMap<Long, ArrayList<BookieSocketAddress>> ensembles = ledgerMetadata.getEnsembles();
                    for (ArrayList<BookieSocketAddress> ensemble : ensembles.values()) {
                        // check if this bookie is supposed to have this ledger
                        if (ensemble.contains(selfBookieAddress)) {
                            result.set(true);
                            return;
                        }
                    }
                }
            }
        });
        return result.get();
    }

    // todo do we need caputure IOExceptions thrown by getLastAddConfirmed in LedgerStorage,
    // which is a part of indexFile
    void addToSuspiciousLedgers(long ledgerId) {
        suspiciousLedgers.add(ledgerId);
    }

    /**
     * Record corrupt index info.
     *
     * @param ledgerId corrupt entry index's ledgerId
     * @param entryId  corrupt entry index's entryId
     */
    void addCorruptIndexItem(long ledgerId, long entryId) {
        corruptEntryIndices.add(Pair.of(ledgerId, entryId));

        Set<Long> values = corruptIndexLedgers.get(ledgerId);
        if (values != null) {
            values.add(entryId);
            corruptIndexLedgers.put(ledgerId, values);
        } else {
            Set<Long> enties = new ConcurrentSkipListSet<>();
            enties.add(entryId);
            corruptIndexLedgers.put(ledgerId, enties);
        }
    }

    void addToSuspiciousEntryLogs(long entryLogId) {
        suspiciousLedgers.add(entryLogId);
    }

    // scan entry log to verify which ledger are corrupt, compare metadata(from metaStore) and entrylog's info
    /**
     * Scan the  suspicious entryLogs and find the corrupt item.
     */
    void scanEntryLogs() {
        // we should first guarantee the ledger index file is not corrupt
        suspiciousEntryLogs.forEach((Consumer<Long>) entryLogId -> {

                    try {
                        entryLogger.scanEntryLog(entryLogId, new EntryLogger.EntryLogScanner() {
                            @Override
                            public boolean accept(long ledgerId) {
                                return true;
//                EntryLogMetadata metadata = null;
//                try {
//                    metadata = entryLogger.getEntryLogMetadata(entryLogId);
//                } catch (Exception e){
//                    LOG.error("Get EntryLogMetadata from {} fail",entryLogId);
//                }
//                return metadata.containsLedger(ledgerId);
                            }

                            //first compare scanned info to entryLogMetada, add suspicous Ledgers;
                            // then delete the ledgers, to trigger URM Replicator(should first check auditor's info)
                            // the above plan has a heavy io and network.
                            // we need a fine granurity way: if just a few entry corrupt, we can retrive it from replica
//                    and update offset info; if a lot entries of a entry log corrupt,
// we should use a more efficient way,
//                    such as let Replicator to deal it by deleting ledgers
                            @Override
                            public void process(long ledgerId, long offset, ByteBuf entry) {
                                synchronized (BookieScanner.this) {
                                    long lid = entry.getLong(entry.readerIndex());
                                    long entryId = entry.getLong(entry.readerIndex() + 8);

                                    if (lid != ledgerId || entryId < -1) {
                                        LOG.warn("Scanning expected ledgerId {}, but found invalid entry "
                                                        + "with ledgerId {} entryId {} at offset {}",
                                                ledgerId, lid, entryId, offset);
                                        // mark the ledger
                                        if (entryId < -1) {
                                            addToSuspiciousLedgers(ledgerId);
                                        } else {
                                            // mark the data as deprecated to avoid IOException,
                                            // and mark the entry, so we can update its offset
                                            // entryLogger.markDataDeprecated,
                                            // todo how to change the entry's state to deprecate effiently?
                                            corruptEntries.add(Pair.of(ledgerId, entryId));
                                        }
                                    }
                                }
                            }
                        });
                    } catch (IOException ioe) {
                        throw new UncheckedIOException(ioe);
                    }
                }
        );

    }
    /**
     *Scan the index files and entryLogs and mark the suspicious ones.
     */
    void scanAndMark() throws IOException{
        scanIndexDirs();
        scanEntryLogs();
    }

    //todo just add to UnderReplicationManager or fix directly, Replicator is designed for URM

    /**
     * Fix the corrupt data using ReplicationWorker which is bound to UnderReplicationManager.
     */
    void fixCorrupt() {

    }

    @VisibleForTesting
    boolean isLedgerInSuspiciousList(long ledgerId) {
        return suspiciousLedgers.contains(ledgerId);
    }

    @VisibleForTesting
    boolean isEntryLogInSuspiciousList(long entryLogId) {
        return suspiciousEntryLogs.contains(entryLogId);
    }

    // todo, which thread execute fixing is better? GCThread?

    /**
     * Trigger Fix action. When suspicious list hit some requirement, we can trigger fix.
     */
    public void triggerFix() {
        if (!fixing.compareAndSet(false, true)) {
            // set compacting flag failed, means compacting is true now
            // indicates that compaction is in progress for this EntryLogId.
            return;
        }
        // Do the actual fixing
        triggerFixIndex();
        triggerFixEntryLog();

        // Mark fixing done
        fixing.set(false);
    }

    /**
     * Trigger fix Index file action.
     */
    void triggerFixIndex() {
        fixer.fixIndexFileMissing();
        fixer.fixCorruptIndexItemByScan();
    }

    /**
     * Trigger fix entry log action.
     */
    void triggerFixEntryLog() {

    }

    /**
     * Shutdown the Scanner.
     */
    void shutdown() {
        fixer.shutdown();
    }

    /**
     * Fixer used to fix partial corrupt ledger, includes index file and entry log.
     */
    class Fixer {
        private ZooKeeper zk = null;
        private LedgerUnderreplicationManager ledgerUnderreplicationManager;
        List<EntryLocation> offsets = new ArrayList<EntryLocation>();
        List<Long> entryLogs = new ArrayList<Long>();

        // todo keep the zk, bk, admin client alive always or create them when fixing?
        public Fixer() throws IOException, KeeperException, InterruptedException {
            zk = ZooKeeperClient.newBuilder().connectString(conf.getZkServers())
                    .sessionTimeoutMs(conf.getZkTimeout()).build();
            try {
                LedgerManagerFactory ledgerManagerFactory = LedgerManagerFactory
                        .newLedgerManagerFactory(conf, zk);
                this.ledgerUnderreplicationManager = ledgerManagerFactory
                        .newLedgerUnderreplicationManager();
            } catch (ReplicationException.CompatibilityException ce) {
                throw new IOException(
                        "CompatibilityException while initializing Fixer", ce);
            }
        }

        //replicate the fragments to new bookie is better, because use current bookie
        // can cause data leak, where old data in current bookie is not released,
        // if use other bookie, current bookie's overReplicated data can be compacted.

        /**
         * When index file is missing, we add the ledger to UnderReplicatedManager
         * if the AutoRecovery is enable, then the always running ReplicationWorker
         * will replicate these ledgers to create new index file in other node.
         */
        private void fixIndexFileMissing() {
            if (null == suspiciousLedgers || suspiciousLedgers.size() == 0) {
                // there is no suspiciousledgers available for this bookie and just
                // ignoring the bookie failures
                LOG.info("There is no ledgers for current bookie: {}", selfBookieAddress);
                return;
            }
            LOG.info("Following ledgers: {} of bookie: {} are identified as corrupt",
                    suspiciousLedgers, selfBookieAddress);
            suspiciousLedgers.forEach(ledgerId -> {
                try {
                    ledgerUnderreplicationManager.markLedgerUnderreplicated(
                            ledgerId, selfBookieAddress.toString());
                    suspiciousLedgers.remove(ledgerId);
                } catch (ReplicationException.UnavailableException ue) {
                    throw new UncheckedIOException(
                            "Failed to publish underreplicated ledger: " + ledgerId
                                    + " of bookie: " + selfBookieAddress, new IOException(ue));
                }
            });
        }

        /**
         * Fix corrupt index item by re-add entry and update their locations.
         */
        private void fixCorruptIndexItemByReplay() throws IOException{

            corruptEntryIndices.forEach(pair -> {
                // todo retrieve entries
                ByteBuf entry = Unpooled.wrappedBuffer(new byte[5]);
                try {
                    long newoffset = entryLogger.addEntry(pair.getLeft(), entry);
                    offsets.add(new EntryLocation(pair.getLeft(), pair.getRight(), newoffset));

                } catch (IOException ioe) {

                }
            });
            flush();
        }

        private void flush() throws IOException {
            if (offsets.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Skipping entry log flushing, as there are no offset!");
                }
                return;
            }

            // Before updating the index, we want to wait until all the re-add entries are flushed into the
            // entryLog
            try {
                entryLogger.flush();
                ledgerStorage.updateEntriesLocations(offsets);
            } finally {
                offsets.clear();
            }
        }

        /**
         * Fix corrupt index item by scan entry log and update their locations.
         */
        private void fixCorruptIndexItemByScan() {
            for (long entryLogId : entryLogs) {
                try {
                    entryLogger.scanEntryLog(entryLogId, getScannerForFixing());
                } catch (LedgerDirsManager.NoWritableLedgerDirException nwlde) {
                    LOG.warn("No writable ledger directory available, aborting compaction", nwlde);
                } catch (IOException ioe) {
                    // if compact entry log throws IOException, we don't want to remove that
                    // entry log. however, if some entries from that log have been re-added
                    // to the entry log, and the offset updated, it's ok to flush that
                    LOG.error("Error compacting entry log. Log won't be deleted", ioe);
                }
            }
        }

        /**
         * Get appropriate entry logs to scan by compare neighbor entry's entryLogId.
         */
        private void chooseEntryLogs() throws ReplicationException.BKAuditException, BKException,
                IOException, InterruptedException, KeeperException{
            for (Map.Entry<Long, Set<Long>> entry: corruptIndexLedgers.entrySet()) {
                long ledgerId = entry.getKey();
                Set<Long> entryIds = entry.getValue();
                for (long entryId : entryIds) {
                    final BookKeeper client = new BookKeeper(new ClientConfiguration(conf),
                            zk);
                    final BookKeeperAdmin admin = new BookKeeperAdmin(client, statsLogger);
                    LedgerHandle lh = null;
                    LedgerFragment fragment = null;
                    lh = admin.openLedgerNoRecovery(ledgerId);
                    long rightEntryLogId = 0 , leftEntryLogId = 0;
                    // 1. find its fragment
                    int selfIndex = 0;
                    LedgerFragment curFragment, leftFragment, rightFragment;
                    try {
                        Pair<LedgerFragment, Integer> pair = getFragment(lh, ledgerId, entryId);
                        curFragment = leftFragment = rightFragment = pair.getLeft();
                        selfIndex = pair.getRight();
                        // continue deal next corrupt
                        if (curFragment == null) {
                            continue;
                        }
                    } catch (Exception e) {
                        continue;
                    }
                    // 2. get its start entry in fragment
                    long left = leftFragment.getFirstStoredEntryId(selfIndex);
                    long leftOffset = 0;
                    // 3. find start entryLogId for scanning
                    try {
                        leftOffset = ledgerCache.getEntryOffset(ledgerId, left);
                        // find the left alive entry's log id
                        while (leftOffset == 0) {
                            //maybe the entry offset in indexFile is corrupt, add into to update its' location later
                            BookieScanner.this.addCorruptIndexItem(ledgerId, left);

                            // move left forward
                            if (left < 1) {
                                break;
                            }
                            Pair<LedgerFragment, Integer> pair = getFragment(lh, ledgerId, left - 1);
                            leftFragment = pair.getLeft();
                            int selfLeftIndex = pair.getRight();
                            left = leftFragment.getFirstStoredEntryId(selfLeftIndex);
                            leftOffset = ledgerCache.getEntryOffset(ledgerId, left);
                        }
                        leftEntryLogId = EntryLogger.logIdForOffset(leftOffset);

                    } catch (IOException ioe) {
                        if (ioe.getCause() instanceof Bookie.NoLedgerException) {
                            //read index file fail, skip this ledger and continue
                            BookieScanner.this.addToSuspiciousLedgers(ledgerId);
                            corruptIndexLedgers.remove(ledgerId);
                            continue;
                        }
                    }
                    // 4. find end entryLogId for scanning
                    long right = rightFragment.getLastStoredEntryId(selfIndex);
                    long rightOffset = 0;
                    try {
                        rightOffset = ledgerCache.getEntryOffset(ledgerId, right);
                        // find the right alive entry's log id
                        while (rightOffset == 0) {
                            //maybe the entry offset in indexFile is corrupt, add into to update its' location later
                            BookieScanner.this.addCorruptIndexItem(ledgerId, left);

                            // move right forward
                            if (right == lh.getLastAddConfirmed()) {
                                break;
                            }
                            Pair<LedgerFragment, Integer> pair = getFragment(lh, ledgerId, right + 1);
                            rightFragment = pair.getLeft();
                            int selfRightIndex = pair.getRight();
                            right = leftFragment.getFirstStoredEntryId(selfRightIndex);
                            rightOffset = ledgerCache.getEntryOffset(ledgerId, left);
                        }
                        rightEntryLogId = EntryLogger.logIdForOffset(rightOffset);

                    } catch (IOException ioe) {
                        if (ioe.getCause() instanceof Bookie.NoLedgerException) {
                            //read index file fail, skip this ledger and continue
                            BookieScanner.this.addToSuspiciousLedgers(ledgerId);
                            corruptIndexLedgers.remove(ledgerId);
                            continue;
                        }
                    }
                    // close lh
                    if (lh != null) {
                        try {
                            lh.close();
                        } catch (BKException bke) {
                            LOG.warn("Couldn't close ledger " + ledgerId, bke);
                        } catch (InterruptedException ie) {
                            LOG.warn("Interrupted closing ledger " + ledgerId, ie);
                            Thread.currentThread().interrupt();
                        }
                    }
                    if (leftOffset != 0 && rightOffset != 0) {
                        for (long j = leftEntryLogId; j < rightEntryLogId; j++) {
                            entryLogs.add(j);
                        }
                    } else if (leftOffset != 0) {
                        entryLogs.add(leftEntryLogId);
                    } else if (rightOffset != 0) {
                        entryLogs.add(rightEntryLogId);
                    }
                }

            }
        }

        /**
         * Find the LedgerFragment of specific entry key.
         * @param ledgerId
         * @param entryId
         * @return
         */
        private Pair<LedgerFragment, Integer> getFragment(LedgerHandle lh, long ledgerId, long entryId) {
            int selfIndex = 0;
            LedgerFragment fragment = null;
            Set<Integer> bookieIndexes = null;
            Long lastEntryKey = null;
            Long curEntryKey = null;
            ArrayList<BookieSocketAddress> lastEnsemble = null;
            for (Map.Entry<Long, ArrayList<BookieSocketAddress>> e : lh
                    .getLedgerMetadata().getEnsembles().entrySet()) {
                if (lastEntryKey != null) {
                    curEntryKey = e.getKey();
//                                entryId is in last fragment
                    if (curEntryKey > entryId) {
                        fragment = new LedgerFragment(lh, lastEntryKey,
                                curEntryKey - 1, bookieIndexes);
                        break;
                    }
                }
                lastEntryKey = e.getKey();
                lastEnsemble = e.getValue();
                bookieIndexes = new HashSet<Integer>();
                for (int i = 0; i < lastEnsemble.size(); i++) {
                    bookieIndexes.add(i);
                    if (lastEnsemble.get(i).equals(selfBookieAddress)) {
                        selfIndex = i;
                    }
                }
            }
            return Pair.of(fragment, selfIndex);
        }

        /**
         *Get scanner for fixing corrupt index file.
         */
        private EntryLogger.EntryLogScanner getScannerForFixing() {
            return new EntryLogger.EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    return entryLogs.contains(ledgerId);
                }

                @Override
                public void process(final long ledgerId, long offset, ByteBuf entry) throws IOException {

                    long entryId = entry.getLong(entry.readerIndex() + 8);
                    if (corruptIndexLedgers.get(ledgerId).contains(entryId)) {
                        offsets.add(new EntryLocation(ledgerId, entryId, offset));
                    }
                }
            };
        }

        /**
         * Shutdown the fixer.
         */
        void shutdown() {
            if (zk != null) {
                try {
                    zk.close();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                zk = null;
            }
        }
    }

}

