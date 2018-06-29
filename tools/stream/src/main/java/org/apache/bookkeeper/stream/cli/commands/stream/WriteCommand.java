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
package org.apache.bookkeeper.stream.cli.commands.stream;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.stream.Stream;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.api.stream.WriteEvent;
import org.apache.bookkeeper.api.stream.WriteResult;
import org.apache.bookkeeper.api.stream.Writer;
import org.apache.bookkeeper.api.stream.WriterConfig;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.router.StringUtf8HashRouter;
import org.apache.bookkeeper.stream.cli.commands.ClientCommand;
import org.apache.bookkeeper.stream.cli.commands.stream.WriteCommand.Flags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.commons.lang3.StringUtils;

/**
 * Commands to append events.
 */
@Slf4j
public class WriteCommand extends ClientCommand<Flags> {

    private static final String NAME = "write";
    private static final String DESC = "Write events into a stream";

    /**
     * Flags of the put command.
     */
    public static class Flags extends CliFlags {

        @Parameter(
            names = {
                "-t", "--timestamp"
            },
            description = "Event Timestamp")
        private long timestamp = -1L;

        @Parameter(
            names = {
                "-k", "--key"
            },
            description = "Event Key")
        private String key = null;

    }

    public WriteCommand() {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(new Flags())
            .withArgumentsUsage("<stream> <event>")
            .build());
    }

    @Override
    protected void run(StorageClient client, Flags flags) throws Exception {
        checkArgument(flags.arguments.size() >= 2,
            "stream and event are not provided");

        String streamName = flags.arguments.get(0);
        String eventData = StringUtils.join(
            flags.arguments.toArray(new String[flags.arguments.size()]),
            " ",
            1,
            flags.arguments.size());

        StreamConfig<String, String> streamConfig = StreamConfig.<String, String>builder()
            .keyCoder(StringUtf8Coder.of())
            .valueCoder(StringUtf8Coder.of())
            .keyRouter(StringUtf8HashRouter.of())
            .build();
        WriterConfig writerConfig = WriterConfig.builder().build();

        try (Stream<String, String> stream = result(client.openStream(streamName, streamConfig))) {
            log.info("Successfully open stream '{}'", streamName);
            try (Writer<String, String> writer = result(stream.openWriter(writerConfig))) {
                log.info("Successfully open writer on writing events to stream '{}'", streamName);
                log.info("Write key, value = {}, {}", flags.key, eventData);
                WriteEvent<String, String> event = writer.eventBuilder()
                    .withKey(flags.key)
                    .withValue(eventData)
                    .withTimestamp(flags.timestamp < 0L ? System.currentTimeMillis() : flags.timestamp)
                    .build();
                WriteResult result = result(writer.write(event));
                spec.console().println("Successfully write event at position : " + result.position());
            }
        }
    }

}
