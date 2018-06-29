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

import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.stream.cli.commands.AdminCommand;
import org.apache.bookkeeper.stream.cli.commands.stream.DeleteStreamCommand.Flags;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Command to delete a stream.
 */
public class DeleteStreamCommand extends AdminCommand<Flags> {

    private static final String NAME = "delete";
    private static final String DESC = "Delete a stream";

    /**
     * Flags for the delete table command.
     */
    public static class Flags extends CliFlags {
    }

    public DeleteStreamCommand() {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(new Flags())
            .withArgumentsUsage("<stream-name>")
            .build());
    }

    @Override
    protected void run(StorageAdminClient admin,
                       BKFlags globalFlags,
                       Flags cmdFlags) throws Exception {
        checkArgument(!cmdFlags.arguments.isEmpty(),
            "Stream name is not provided");

        String streamName = cmdFlags.arguments.get(0);

        spec.console().println("Deleting stream '" + streamName + "' ...");
        try {
            result(admin.deleteStream(globalFlags.namespace, streamName));
            spec.console().println("Successfully deleted stream '" + streamName + "'.");
        } catch (StreamNotFoundException snfe) {
            spec.console().println("Stream '" + streamName + "' is not found");
        }
    }

}
