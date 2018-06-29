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
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.stream.Position;
import org.apache.bookkeeper.api.stream.ReadEvent;
import org.apache.bookkeeper.api.stream.ReadEvents;
import org.apache.bookkeeper.api.stream.Reader;
import org.apache.bookkeeper.api.stream.ReaderConfig;
import org.apache.bookkeeper.api.stream.Stream;
import org.apache.bookkeeper.api.stream.StreamConfig;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.router.StringUtf8HashRouter;
import org.apache.bookkeeper.stream.cli.commands.ClientCommand;
import org.apache.bookkeeper.stream.cli.commands.stream.ReadCommand.Flags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Command to read events from stream.
 */
public class ReadCommand extends ClientCommand<Flags> {

    private static final String NAME = "read";
    private static final String DESC = "Read events from a stream";

    /**
     * Flags for the read command.
     */
    public static class Flags extends CliFlags {

        @Parameter(
            names = {
                "-n", "--num-events"
            },
            description = "Number of events to read, 0 means to read forever")
        private int numEvents = 0;

    }

    public ReadCommand() {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(new Flags())
            .withArgumentsUsage("<stream>")
            .build());
    }

    @Override
    protected void run(StorageClient client, Flags flags) throws Exception {
        checkArgument(flags.arguments.size() >= 1,
            "stream is not provided");

        String streamName = flags.arguments.get(0);

        StreamConfig<String, String> streamConfig = StreamConfig.<String, String>builder()
            .keyCoder(StringUtf8Coder.of())
            .valueCoder(StringUtf8Coder.of())
            .keyRouter(StringUtf8HashRouter.of())
            .build();
        ReaderConfig readerConfig = ReaderConfig.builder()
            .maxReadAheadCacheSize(1024 * 1024)
            .build();

        int numReads = 0;
        boolean running = true;
        try (Stream<String, String> stream = result(client.openStream(streamName, streamConfig))) {
            try (Reader<String, String> reader = result(stream.openReader(readerConfig, Position.TAIL))) {
                while (running) {
                    ReadEvents<String, String> readEvents = reader.readNext(100, TimeUnit.MILLISECONDS);
                    if (null == readEvents) {
                        continue;
                    }
                    try {
                        ReadEvent<String, String> event = readEvents.next();
                        while (null != event) {
                            spec.console().println("--- key = '" + event.key()
                                + "', timestamp = " + event.timestamp() + ", pos = " + event.position() + " ---");
                            spec.console().println(event.value());
                            spec.console();
                            event.close();

                            ++numReads;
                            if (flags.numEvents > 0 && numReads >= flags.numEvents) {
                                running = false;
                                break;
                            } else {
                                event = readEvents.next();
                            }
                        }
                    } finally {
                        readEvents.close();
                    }
                }
            }
        }
    }

}
