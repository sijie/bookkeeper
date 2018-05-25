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

package org.apache.bookkeeper.api;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * Api Code.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@RequiredArgsConstructor
@Getter
public enum Code {

    OK(0),

    // 4xx: client error3
    BAD_REQUEST(400),
    ILLEGAL_OP(403),
    INVALID_ARGUMENT(412),

    // 5xx: server errors
    INTERNAL_ERROR(500),
    NOT_IMPLEMENTED(501),

    // 6xx: unexpected
    UNEXPECTED(600),

    // 9xx: revisions, versions
    BAD_VERSION(900),
    BAD_REVISION(901),
    SMALLER_REVISION(902),

    // 21xx: Stream API
    END_OF_STREAM(2106),

    // 6xxx: KV API
    INVALID_KEY(6000),
    KEY_EXISTS(6001),
    KEY_NOT_FOUND(6002);

    private final int code;

}
