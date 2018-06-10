/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.storage.impl.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.bookkeeper.stream.proto.storage.ReadRequest;
import org.apache.bookkeeper.stream.proto.storage.ReadResponse;
import org.apache.bookkeeper.stream.proto.storage.ReadServiceGrpc.ReadServiceImplBase;
import org.apache.bookkeeper.stream.storage.api.stream.StreamStore;
import org.apache.bookkeeper.stream.storage.impl.stream.ReadSession;

/**
 * Grpc based read service.
 */
public class GrpcStreamReadService extends ReadServiceImplBase {

    private final StreamStore streamStore;

    public GrpcStreamReadService(StreamStore streamStore) {

        this.streamStore = streamStore;
    }

    @Override
    public StreamObserver<ReadRequest> read(StreamObserver<ReadResponse> respObserver) {
        return new ReadSession(streamStore, respObserver);
    }
}
