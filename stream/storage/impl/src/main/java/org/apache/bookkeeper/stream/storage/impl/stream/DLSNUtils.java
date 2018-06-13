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

package org.apache.bookkeeper.stream.storage.impl.stream;

import org.apache.distributedlog.DLSN;

/**
 * Utils to handle {@link org.apache.distributedlog.DLSN}.
 */
final class DLSNUtils {

    private DLSNUtils() {}

    static long getRangeSeqNum(DLSN dlsn) {
        long logSegmentId = dlsn.getLogSegmentSequenceNo();
        long entryId = dlsn.getEntryId();

        // Combine log segment id and entry id to form range seq num
        // Use less than 32 bits to represent entry id since it will get
        // rolled over way before overflowing the max int range
        long seqNum = (logSegmentId << 28) | entryId;
        return seqNum;
    }

    static DLSN getDLSN(long seqNum) {
        // Demultiplex log segment id and entry id from seqNum
        long logSegmentId = seqNum >>> 28;
        long entryId = seqNum & 0x0F_FF_FF_FFL;

        return new DLSN(logSegmentId, entryId, 0);
    }


}
