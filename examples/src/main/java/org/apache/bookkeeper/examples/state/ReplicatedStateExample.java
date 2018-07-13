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

package org.apache.bookkeeper.examples.state;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.RateLimiter;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bookkeeper.examples.state.ReplicatedState.SerDe;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main program to run a replicated set.
 */
public class ReplicatedStateExample {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateExample.class);

    private static class StringUtf8SerDe implements SerDe<String> {

        @Override
        public byte[] serialize(String value) {
            return value.getBytes(UTF_8);
        }

        @Override
        public String deserialize(byte[] data) {
            return new String(data, UTF_8);
        }
    }

    private static class MainArgs {
        @Parameter(
            names = { "-u", "--dlog-uri" },
            required = true,
            description = "distributedlog service uri")
        String dlogUri;

        @Parameter(
            names = { "-n", "--log-name" },
            required = true,
            description = "distributedlog stream name for replicating the state")
        String logName;
    }

    public static void main(String[] args) {
        // parse the commandline
        MainArgs mainArgs = new MainArgs();
        JCommander commander = new JCommander();
        commander.setProgramName("ReplicatedSet");
        commander.addObject(mainArgs);
        try {
            commander.parse(args);
        } catch (Exception e) {
            System.err.println("Error : " + e.getMessage());
            commander.usage();
            Runtime.getRuntime().exit(-1);
            return;
        }

        // build the distributedlog configuration
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            // set the timeout to "infinite" wait until lock being released
            .setLockTimeout(-1)
            // disable immediate flush and enable periodical flush to
            // get a good tradeoff between latency and throughput
            .setImmediateFlushEnabled(false)
            .setPeriodicFlushFrequencyMilliSeconds(2)
            .setOutputBufferSize(256 * 1024) // 256KB
            // set readahead settings for readers
            .setReadAheadMaxRecords(10000)
            .setReadAheadBatchSize(10);

        // build the distributedlog namespace instance
        try (Namespace namespace = NamespaceBuilder.newBuilder()
            .uri(URI.create(mainArgs.dlogUri))
            .conf(conf)
            .statsLogger(NullStatsLogger.INSTANCE)
            .build()) {

            try (ReplicatedState<String> replicatedSet = new ReplicatedState<>(
                namespace, mainArgs.logName, new StringUtf8SerDe()
            )) {
                replicatedSet.start();

                // after a replicated set is started, it is usable now
                System.out.println("=== Testing Replicated Set ===");
                System.out.println();
                testReplicatedSet(replicatedSet);
            }
        } catch (Exception e) {
            log.error("Encountered exceptions on running replicated set", e);
        }
    }

    private static void testReplicatedSet(ReplicatedState<String> replicatedSet)
            throws ExecutionException, InterruptedException {
        RateLimiter limiter = RateLimiter.create(1);

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            System.out.println("--- begin test run : " + i + " ---");
            System.out.println();

            int numValues = ThreadLocalRandom.current().nextInt(32);
            for (int j = 0; j < numValues; j++) {
                limiter.acquire();
                String value = "run-" + i + "-value-" + j;
                replicatedSet.addValue(value).get();
                System.out.println("add value : " + value);
            }

            for (int j = 0; j < numValues; j++) {
                limiter.acquire();
                String value = "run-" + i + "-value-" + j;
                boolean removed = replicatedSet.removeValue(value).get();
                System.out.println("remove value '" + value + "' : " + removed);
            }

            System.out.println();
            System.out.println("--- end test run : " + i + " ---");
        }
    }

}
