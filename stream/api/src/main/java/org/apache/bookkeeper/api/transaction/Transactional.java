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

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * An interface represents a transactional object.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Transactional {

  /**
   * Returns the unique ID that can be used to identify this transaction.
   *
   * @return transaction id.
   */
  TxnID getTxnId();

  /**
   * Returns the status of this transaction.
   *
   * <p>The implementation should not block any threads.
   *
   * @return the status of this transaction.
   */
  TxnStatus getStatus();

  /**
   * Commit all the operations that were attached to this transaction previously.
   *
   * <p>The operations attached to this transaction will either succeed together or fail together.
   * There is no partial state in between.
   *
   * <p>This call is non-blocking. Application can listen on the future returned by this method to
   * learn the result of this transaction. The future will tell what is the final transaction status
   * after this call and a boolean flag indicating whether this operation changed the status of transaction.
   * For example, if a transaction was already in {@link TxnStatus#ABORTED} status, the call will return
   * {@link TxnStatus#ABORTED} and False. {@link TxnStatus#ABORTED} means the transaction is already
   * aborted and {@code False} means this call doesn't change this transaction.
   *
   * @return callback to listen on the commit result.
   */
  CompletableFuture<TxnResult> commit();

  /**
   * Abort all the operations that were attached to this transaction previously.
   *
   * <p>Similar as {@link #commit()}, this call is non-blocking. Application can also listen on the future
   * returned by this method to learn the result of this transaction. The result contains the final status
   * of this transaction and a flag indicating whether this operation changed the status of the transaction.
   *
   * @return callback to listen on the abort result.
   */
  CompletableFuture<TxnResult> abort();

}
