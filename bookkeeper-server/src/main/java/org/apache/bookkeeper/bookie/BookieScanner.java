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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
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
class BookieScanner {
    private static final Logger LOG = LoggerFactory.getLogger(BookieScanner.class);

    private final ServerConfiguration conf;
    private final EntryLogger entryLogger;
    final CompactableLedgerStorage ledgerStorage;
    final LedgerCache ledgerCache;
    final LedgerManager ledgerManager;
    private final BookieSocketAddress selfBookieAddress;

    // includes ledgers whose index file is missing or losing entry in entry log file
    private final ConcurrentOpenHashSet<Long> suspiciousLedgers = new ConcurrentOpenHashSet<>();
    // contains corrupt index in index file: entryKeyOffset -/-> entry
    private final ConcurrentHashMap<Long, Set<Long>> corruptIndexLedgers = new ConcurrentHashMap<>();
    // corrupt entries in entryLog
    private final ConcurrentOpenHashSet<Pair<Long, Long>> corruptEntries =
            new ConcurrentOpenHashSet<>();
    // todo use what structures to store ledgers (heap?) and cursor ?
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, LedgerFragment>>
            corruptFragments = new ConcurrentHashMap<>();
    private final ConcurrentOpenHashSet<Long> suspiciousEntryLogs = new ConcurrentOpenHashSet<>();
    private final ConcurrentOpenHashSet<Long> corruptLedgers = new ConcurrentOpenHashSet<>();
    private final ConcurrentOpenHashSet<Long> corruptEntryLogs = new ConcurrentOpenHashSet<>();
    private int scanCursor;
    // parameter for scanning, should pass to GCThread to control the scan frequency.
    private int scanInterval;
    private int persistentInterval;

    // flag to indicate fix is in progress
    final AtomicBoolean fixing = new AtomicBoolean(false);
    final Fixer fixer;
    // Expose Stats
    private final StatsLogger statsLogger;
    private final Counter scannedLogsCounter;
    private final Counter suspiciousLogsCounter;
    private final Counter scannedIndicesCounter;
    private final Counter suspiciousIndicesCounter;
    // Scanner Operation Latency Stats
    private final OpStatsLogger scanIndexDirsStats;
    private final OpStatsLogger scanEntryLogsStats;
    private final OpStatsLogger fixIndicesStats;
    private final OpStatsLogger fixEntryLogsStats;

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
        // Expose Stats
        scannedLogsCounter = statsLogger.getCounter("scannedLogsCounter");
        suspiciousLogsCounter = statsLogger.getCounter("suspiciousLogsCounter");
        scannedIndicesCounter = statsLogger.getCounter("scannedIndicesCounter");
        suspiciousIndicesCounter = statsLogger.getCounter("suspiciousIndicesCounter");
        scanIndexDirsStats = statsLogger.getOpStatsLogger("scanIndexDirsStats");
        scanEntryLogsStats = statsLogger.getOpStatsLogger("scanEntryLogsStats");
        fixIndicesStats = statsLogger.getOpStatsLogger("fixIndicesStats");
        fixEntryLogsStats = statsLogger.getOpStatsLogger("fixEntryLogsStats");
    }

    /**
     * Scan Index dirs to find suspicious ledger: find those should in local but not.
     * Check ledger in (set(ledgersInMetaStore) - set(ledgersInLocal))
     * check whether ledger use this bk as part of its ensemble.
     */
    private void scanIndexDirs() throws IOException {
        long beforeScanNanos = MathUtils.nowInNano();
        boolean success = false;
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
            // check ledger's ensemble whether contains local bk
            ledgersToCheck.forEach(ledgerId -> {
                if (isSuspiciousLedger(ledgerId)) {
                    addToSuspiciousLedgers(ledgerId);
                }
            });
            success = true;
        } catch (Throwable t) {
            // ignore exception, check next time
            LOG.warn("Exception when iterating over the metadata {}", t);
        } finally {
            long elapsedNanos = MathUtils.elapsedNanos(beforeScanNanos);
            if (success) {
                scanIndexDirsStats.registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
            } else {
                scanIndexDirsStats.registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
            }
        }
    }

    /**
     * Check whether the ledger is suspicious.
     */
    private boolean isSuspiciousLedger(long ledgerId) {
        scannedIndicesCounter.inc();
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
        suspiciousIndicesCounter.inc();
        suspiciousLedgers.add(ledgerId);
    }

    /**
     * Record corrupt index info.
     *
     * @param ledgerId corrupt entry index's ledgerId
     * @param entryId  corrupt entry index's entryId
     */
    void addCorruptIndexItem(long ledgerId, long entryId) {
        Set<Long> values = corruptIndexLedgers.get(ledgerId);
        if (values != null) {
            values.add(entryId);
            corruptIndexLedgers.put(ledgerId, values);
        } else {
            Set<Long> enties = new TreeSet<>();
            enties.add(entryId);
            corruptIndexLedgers.put(ledgerId, enties);
        }
    }

    //todo add a method to remove the fixed entryLog from suspicious list, and call it after fix
    void addToSuspiciousEntryLogs(long entryLogId) {
        suspiciousLogsCounter.inc();
        suspiciousLedgers.add(entryLogId);
    }

    // scan entry log to verify which ledger are corrupt, compare metadata(from metaStore) and entrylog's info
    /**
     * Scan the  suspicious entryLogs and find the corrupt item.
     * Transfer entryLog's problem to suspicious ledger problem.
     */
    void scanEntryLogs() {
        suspiciousEntryLogs.forEach(entryLogId -> extractEntryLogMetadataAndFindCorrupt(entryLogId));
    }


    //first compare scanned info to entryLogMetada, add suspicous Ledgers;
    // then delete the ledgers, to trigger URM Replicator(should first check auditor's info)
    // the above plan has a heavy io and network.
    // we need a fine granurity way: if just a few entry corrupt, we can retrive it from replica
//                    and update offset info; if a lot entries of a entry log corrupt,
// we should use a more efficient way,
//                    such as let Replicator to deal it by deleting ledgers

    /**
     * Extract new meta and compare it with old meta if it exists.
     * Using an no exception thrown way to construct entry log meta and find the corrupt ledger.
     * @param entryLogId
     * @return
     */
    private void extractEntryLogMetadataAndFindCorrupt(long entryLogId) {
        scannedLogsCounter.inc();
        final EntryLogMetadata newMeta = new EntryLogMetadata(entryLogId);
        try {
            // Read through the entry log file and extract the entry log meta
            entryLogger.scanEntryLog(entryLogId, new EntryLogger.EntryLogScanner() {
                @Override
                public void process(long ledgerId, long offset, ByteBuf entry) {
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
                            // and mark the entry, so we can update its offset
                            // entryLogger.markDataDeprecated,
                            corruptEntries.add(Pair.of(ledgerId, entryId));
                        }
                        // mark the data as deprecated to avoid IOException, and then extractMetaByScan
                        // to get entryLog's new meta
//                                        entryLogger.changeEntryState(long offset, )
                        return;
                    }
                    //todo checksum of entry, an design: use a map to keep ledgerId -> MacManager

                    // only add new entry size of a ledger to entry log meta when all check passed
                    newMeta.addLedgerSize(ledgerId, entry.readableBytes() + 4);
                }

                @Override
                public boolean accept(long ledgerId) {
                    return ledgerId > 0;
                }
            });
        } catch (IOException ioe) {
            LOG.error("scanEntryLog failed for entryLogId: {}", entryLogId, ioe);
            //todo add to a  error entry log list whose item has several times error.
            suspiciousEntryLogs.add(entryLogId);
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("New entry log meta data entryLogId: {}, meta: {}", entryLogId, newMeta);
        }
        // compare with old meta
        try {
            EntryLogMetadata oldMeta = entryLogger.getEntryLogMetadata(entryLogId);
            oldMeta.getLedgersMap().forEach((ledgerId, size) -> {
                if (newMeta.containsLedger(ledgerId)) {
                    if (newMeta.getLedgersMap().get(ledgerId) != size) {
                        suspiciousLedgers.add(ledgerId);
                    }
                } else {
                    suspiciousLedgers.add(ledgerId);
                }
            });
        } catch (IOException ioe) {
            LOG.error("get entry log meta data failed for entryLogId: {}", entryLogId, ioe);
            //todo add to a  error entry log list whose item has several times error.
            suspiciousEntryLogs.add(entryLogId);
        }
        // compare with LedgerManager's ledger meta,todo need fine granularity, eg. ledgerFragment
    }

    /**
     *Scan the index files and entryLogs and mark the suspicious ones.
     */
    void scanAndMark() throws IOException{
        scanIndexDirs();
        scanEntryLogs();
    }

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
     * Caller should guarantee only one thread access, as the `offsets` are designed for one thread.
     */
    class Fixer {
        final BookKeeper client;
        final BookKeeperAdmin admin;
        private ZooKeeper zk = null;
        private LedgerUnderreplicationManager ledgerUnderreplicationManager;
        List<EntryLocation> offsets = new ArrayList<EntryLocation>();
        Set<Long> candidateEntryLogs = new HashSet<>();

        // the caller of Fixer should guarantee only one thread calling
        Fixer() throws Exception {
            zk = ZooKeeperClient.newBuilder().connectString(conf.getZkServers())
                    .sessionTimeoutMs(conf.getZkTimeout()).build();
            try (RegistrationManager rm = RegistrationManager.instantiateRegistrationManager(conf)) {
                try (LedgerManagerFactory mFactory =
                             LedgerManagerFactory.newLedgerManagerFactory(conf, rm.getLayoutManager())) {
                    this.ledgerUnderreplicationManager = mFactory
                            .newLedgerUnderreplicationManager();
                }
            }
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.addConfiguration(conf);
            client = new BookKeeper(clientConfiguration, zk);
            admin = new BookKeeperAdmin(client, statsLogger);
        }
        //replicate the fragments to new bookie is better, because using current bookie
        // can cause data leak, where old data in current bookie is not released,
        // if use other bookie, current bookie's overReplicated data can be compacted.

        /**
         * When index file is missing, we add the ledger to UnderReplicatedManager.
         * if the AutoRecovery is enable, then the always running ReplicationWorker
         * will replicate these ledgers to create new index file in other node.
         */
        //todo remove the index file from suspicious list when URM fixed it.
        void fixIndexFileMissing() {
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
         * fixing plan A 4 corrupt index item
         */
        void fixCorruptIndexItemByReplay() throws Exception{
            ConcurrentHashMap<Long, Set<Long>> failedIndexLedgers = new ConcurrentHashMap<>();
            for (Map.Entry<Long, Set<Long>> entry: corruptIndexLedgers.entrySet()) {
                long ledgerId = entry.getKey();
                Set<Long> entryIds = entry.getValue();
                Set<Long> failedEntryIds = new TreeSet<>();
                //todo consecutive enties would be better
                entryIds.forEach(entryId -> {
                    try {
                        Iterator<LedgerEntry> entries =
                                admin.readEntries(ledgerId, entryId, entryId).iterator();
                        if (entries.hasNext()) {
                            LedgerEntry ledgerEntry = entries.next();
                            ByteBuf data = ledgerEntry.getEntryBuffer();
                            ByteBuf bb = Unpooled.buffer(8 + 8 + data.capacity());
                            bb.writeLong(ledgerId);
                            bb.writeLong(entryId);
                            bb.writeBytes(data);
                            long newoffset = entryLogger.addEntry(ledgerId, bb);
                            offsets.add(new EntryLocation(ledgerId, entryId, newoffset));
                        } else {
                            failedEntryIds.add(entryId);
                        }
                    } catch (Exception e) {
                        LOG.error("occured exception when fixing", e);
                        failedEntryIds.add(entryId);
                    }
                });
                if (failedEntryIds.size() != 0) {
                    failedIndexLedgers.put(ledgerId, failedEntryIds);
                }
            }
            flush();
        }

        /**
         * Fix corrupt index item by scan entry log and update their locations.
         * fixing plan B for corrupt index item
         */
        void fixCorruptIndexItemByScan() {
            try {
                chooseEntryLogs();
            } catch (Exception e) {
                LOG.error("occur error when preparing entryLogs for fixing Plan B", e);
                return;
            }
            for (long entryLogId : candidateEntryLogs) {
                try {
                    entryLogger.scanEntryLog(entryLogId, getScannerForFixing(entryLogId));
                } catch (LedgerDirsManager.NoWritableLedgerDirException nwlde) {
                    LOG.warn("No writable ledger directory available, aborting fixing", nwlde);
                } catch (IOException ioe) {
                    // if compact entry log throws IOException, we don't want to remove that
                    // entry log. however, if some entries from that log have been re-added
                    // to the entry log, and the offset updated, it's ok to flush that
                    LOG.error("Error scaning entry log.", ioe);
                }
            }
            try {
                flush();
            } catch (IOException ioe) {
                LOG.error("Error flushing new offsets for entryLogs:{}", candidateEntryLogs, ioe);
            }
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

        // todo improve logic efficiently
        /**
         * Get appropriate entry logs to scan by compare neighbor entry's entryLogId.
         */
        private void chooseEntryLogs() throws ReplicationException.BKAuditException, BKException,
                IOException, InterruptedException, KeeperException{
            for (Map.Entry<Long, Set<Long>> entry: corruptIndexLedgers.entrySet()) {
                long ledgerId = entry.getKey();
                Set<Long> entryIds = entry.getValue();
                LedgerHandle lh = null;
                lh = admin.openLedgerNoRecovery(ledgerId);
                for (long entryId : entryIds) {
                    long rightEntryLogId = 0 , leftEntryLogId = 0;
                    // 1. find its fragment
                    int selfIndex = 0;
                    LedgerFragment curFragment, leftFragment, rightFragment;
                    try {
                        Pair<LedgerFragment, Integer> pair = getFragment(lh, entryId);
                        curFragment = leftFragment = rightFragment = pair.getLeft();
                        selfIndex = pair.getRight();
                        // continue deal next corrupt
                        if (curFragment == null) {
                            continue;
                        }
                    } catch (Exception e) {
                        LOG.warn("Occur Exception: {} when getFragment", e);
                        continue;
                    }
                    LOG.warn("chooseEntryLogs for {} : {} in fragment: {}", ledgerId, entryId, leftFragment);
                    // 2. get its start entry in fragment
                    long left = leftFragment.getFirstStoredEntryId(selfIndex);
                    long leftOffset = 0;
                    // 3. find start entryLogId for scanning(because the fragment may relative to several entrylogs)
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
                            Pair<LedgerFragment, Integer> pair = getFragment(lh, left - 1);
                            leftFragment = pair.getLeft();
                            int selfLeftIndex = pair.getRight();
                            left = leftFragment.getFirstStoredEntryId(selfLeftIndex);
                            leftOffset = ledgerCache.getEntryOffset(ledgerId, left);
                        }
                        leftEntryLogId = EntryLogger.logIdForOffset(leftOffset);
                        LOG.info("leftEntryLog id is {} ", leftEntryLogId);
                    } catch (IOException ioe) {
                        LOG.info("occur exception: {}, when acculate leftEntryLog id", ioe);
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
                            Pair<LedgerFragment, Integer> pair = getFragment(lh, right + 1);
                            rightFragment = pair.getLeft();
                            int selfRightIndex = pair.getRight();
                            right = leftFragment.getFirstStoredEntryId(selfRightIndex);
                            rightOffset = ledgerCache.getEntryOffset(ledgerId, left);
                        }
                        rightEntryLogId = EntryLogger.logIdForOffset(rightOffset);
                        LOG.info("rightEntryLog id is {} ", rightEntryLogId);
                    } catch (IOException ioe) {
                        if (ioe.getCause() instanceof Bookie.NoLedgerException) {
                            //read index file fail, skip this ledger and continue
                            BookieScanner.this.addToSuspiciousLedgers(ledgerId);
                            corruptIndexLedgers.remove(ledgerId);
                            continue;
                        }
                    }
                    for (long j = leftEntryLogId; j <= rightEntryLogId; j++) {
                        candidateEntryLogs.add(j);
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

            }
        }

        /**
         * Find the LedgerFragment of specific entry key.
         * @param entryId
         * @return
         */
        private Pair<LedgerFragment, Integer> getFragment(LedgerHandle lh, long entryId) {
            int selfIndex = 0;
            LedgerFragment lastFragment = null;
            LedgerFragment fragment = null;
            int location = 0;
            Set<Integer> bookieIndexes = null;
            Long lastEntryKey = null;
            Long curEntryKey = null;
            ArrayList<BookieSocketAddress> lastEnsemble = null;
            long lac = lh.getLastAddConfirmed();
            if (lac < entryId) {
                LOG.error("Ledger {} 's lac is not less than entryId:{}", lh.getId(), entryId);
            }
            for (Map.Entry<Long, ArrayList<BookieSocketAddress>> e : lh
                    .getLedgerMetadata().getEnsembles().entrySet()) {
                // the entry is not in the first fragment
                if (lastEntryKey != null) {
                    curEntryKey = e.getKey();
//                                entryId is in previous fragment
                    if (curEntryKey > entryId) {
                        fragment = new LedgerFragment(lh, lastEntryKey,
                                curEntryKey - 1, bookieIndexes);
                        location = 1;
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

                lac = lh.getLastAddConfirmed();
                // the lac is not correct case
                if (!lh.isClosed() && lac < lastEntryKey) {
                    LOG.error("Ledger {} is open and the lac is not set yet", lh.getId());
                } else {
                    lastFragment = new LedgerFragment(lh, lastEntryKey,
                            lac, bookieIndexes);
                }
            }
            if (location == 1) {
                return Pair.of(fragment, selfIndex);
            } else {
                return Pair.of(lastFragment, selfIndex);
            }
        }

        /**
         *Get scanner for fixing corrupt index file.
         */
        private EntryLogger.EntryLogScanner getScannerForFixing(long entryLogId) {
            return new EntryLogger.EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    return corruptIndexLedgers.keySet().contains(ledgerId);
                }

                @Override
                public void process(final long ledgerId, long offset, ByteBuf entry) throws IOException {
                    long entryId = entry.getLong(entry.readerIndex() + 8);
                    // because the offset is relative to entryLogFile, and the offset is after `entrySize`
                    if (corruptIndexLedgers.get(ledgerId).contains(entryId)) {
                        offsets.add(new EntryLocation(ledgerId, entryId, ((entryLogId << 32L) | (offset + 4))));
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

