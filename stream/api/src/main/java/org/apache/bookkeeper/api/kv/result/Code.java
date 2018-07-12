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

package org.apache.bookkeeper.api.kv.result;

/**
 * Status Code.
 */
public enum Code {

    OK(org.apache.bookkeeper.api.Code.OK),
    INTERNAL_ERROR(org.apache.bookkeeper.api.Code.INTERNAL_ERROR),
    INVALID_ARGUMENT(org.apache.bookkeeper.api.Code.INVALID_ARGUMENT),
    ILLEGAL_OP(org.apache.bookkeeper.api.Code.ILLEGAL_OP),
    UNEXPECTED(org.apache.bookkeeper.api.Code.UNEXPECTED),
    BAD_REVISION(org.apache.bookkeeper.api.Code.BAD_REVISION),
    SMALLER_REVISION(org.apache.bookkeeper.api.Code.SMALLER_REVISION),
    KEY_NOT_FOUND(org.apache.bookkeeper.api.Code.KEY_NOT_FOUND),
    KEY_EXISTS(org.apache.bookkeeper.api.Code.KEY_EXISTS);

    private final org.apache.bookkeeper.api.Code code;

    Code(org.apache.bookkeeper.api.Code code) {
        this.code = code;
    }

    int getCode() {
        return code.getCode();
    }

    org.apache.bookkeeper.api.Code code() {
        return code;
    }

}
