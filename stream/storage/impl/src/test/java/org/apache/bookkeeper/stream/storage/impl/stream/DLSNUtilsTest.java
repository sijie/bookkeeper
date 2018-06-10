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

import static org.apache.bookkeeper.stream.storage.impl.stream.DLSNUtils.getDLSN;
import static org.apache.bookkeeper.stream.storage.impl.stream.DLSNUtils.getRangeSeqNum;
import static org.junit.Assert.assertEquals;

import org.apache.distributedlog.DLSN;
import org.junit.Test;

/**
 * Unit test {@link DLSNUtils}.
 */
public class DLSNUtilsTest {

    @Test
    public void testSeqNumToDLSN() {
        DLSN zeroSlotDlsn = new DLSN(1234L, 3456L, 0L);
        assertEquals(zeroSlotDlsn, getDLSN(getRangeSeqNum(zeroSlotDlsn)));

        DLSN noneZeroSlotDlsn = new DLSN(1234L, 3456L, 768L);
        assertEquals(
            zeroSlotDlsn,
            getDLSN(getRangeSeqNum(noneZeroSlotDlsn)));
    }

}
