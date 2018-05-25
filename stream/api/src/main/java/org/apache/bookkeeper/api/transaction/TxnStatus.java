/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.api.transaction;

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * Represent status of a transactional operation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum TxnStatus {

    NONE(0),
    OPEN(1),
    SEALING(2),
    SEALED(3),
    COMMITTING(4),
    COMMITTED(5),
    ABORTING(6),
    ABORTED(7);

    int code;

    TxnStatus(int code) {
        this.code = code;
    }

    int code() {
        return code;
    }


}
