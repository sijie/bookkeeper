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

package org.apache.bookkeeper.tests.integration.stream;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.ReadEvent;
import org.apache.bookkeeper.api.stream.ReadEvents;
import org.apache.bookkeeper.api.stream.Reader;
import org.apache.bookkeeper.api.stream.ReaderConfig;
import org.apache.bookkeeper.api.stream.Stream;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.WriteEvent;
import org.apache.bookkeeper.api.stream.Writer;
import org.apache.bookkeeper.api.stream.WriterConfig;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.coder.VarIntCoder;
import org.apache.bookkeeper.common.router.IntHashRouter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Integration test for stream service.
 */
@Slf4j
public class StreamClientTest extends StreamClusterTestBase {

    @Rule
    public final TestName testName = new TestName();

    private final String namespace = "default";
    private StorageClient storageClient;

    @Before
    public void setup() {
        StorageClientSettings settings = newStorageClientSettings();
        storageClient = createStorageClient(settings, namespace);
    }

    @After
    public void teardown() {
        if (null != storageClient) {
            storageClient.close();
        }
    }

    @Test
    public void testStreamAPI() throws Exception {
        StreamConfig<Integer, String> streamConfig = StreamConfig.<Integer, String>builder()
            .keyCoder(VarIntCoder.of())
            .valueCoder(StringUtf8Coder.of())
            .keyRouter(IntHashRouter.of())
            .build();
        Stream<Integer, String> stream = result(storageClient.openStream(TEST_STREAM, streamConfig));

        Writer<Integer, String> writer = result(stream.openWriter(WriterConfig.builder().build()));
        int numEvents = 100;
        Map<Integer, String> eventsWritten = new HashMap<>();
        for (int i = 0; i < numEvents; i++) {
            int key = ThreadLocalRandom.current().nextInt();
            String value = String.format("event-%d-%10d", i, key);
            eventsWritten.put(key, value);
            WriteEvent<Integer, String> writeEvent = writer.eventBuilder()
                .withKey(key)
                .withValue(value)
                .withTimestamp(i)
                .build();
            result(writer.write(writeEvent));
            log.info("Write event : key = {}, value = {}, timestamp = {}",
                key, value, i);
        }

        Reader<Integer, String> reader = result(stream.openReader(ReaderConfig.builder().build(), Position.TAIL));
        while (!eventsWritten.isEmpty()) {
            ReadEvents<Integer, String> events = reader.readNext(100, TimeUnit.MILLISECONDS);
            if (null == events) {
                continue;
            }
            try {
                log.info("Receive a ReadEvents with {} events", events.numEvents());
                ReadEvent<Integer, String> event = events.next();
                while (null != event) {
                    log.info("Received event : key = {}, value = {}, timestamp = {}, pos = {}",
                        event.key(), event.value(), event.timestamp(), event.position());
                    assertNotNull(event.key());
                    assertNotNull(event.timestamp());
                    int key = event.key();
                    long timestamp = event.timestamp();
                    String writeValue = eventsWritten.remove(key);
                    assertNotNull(writeValue);
                    String expectedValue = String.format("event-%d-%10d", timestamp, key);
                    assertEquals(expectedValue, writeValue);
                    event.close();

                    event = events.next();
                }
            } finally {
                events.close();
            }
        }
        assertNull(reader.readNext(100, TimeUnit.MILLISECONDS));

        writer.close();
        reader.close();
    }

}
