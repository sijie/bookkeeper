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

package org.apache.bookkeeper.api.stream;

import java.io.Serializable;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * Event position in the stream.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Position extends Serializable {

    Position HEAD = new Position() {

        private static final long serialVersionUID = -3383415678170827794L;

        @Override
        public int hashCode() {
            return "HEAD".hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return HEAD == obj;
        }

        @Override
        public String toString() {
            return "TAIL";
        }
    };

    Position TAIL = new Position() {

        private static final long serialVersionUID = 6562771001228524951L;

        @Override
        public int hashCode() {
            return "TAIL".hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return TAIL == obj;
        }

        @Override
        public String toString() {
            return "TAIL";
        }
    };

}
