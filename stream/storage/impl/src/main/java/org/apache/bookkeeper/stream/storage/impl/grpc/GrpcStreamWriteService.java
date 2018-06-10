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
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.storage.WriteRequest;
import org.apache.bookkeeper.stream.proto.storage.WriteResponse;
import org.apache.bookkeeper.stream.proto.storage.WriteServiceGrpc.WriteServiceImplBase;
import org.apache.bookkeeper.stream.storage.api.stream.StreamStore;
import org.apache.bookkeeper.stream.storage.impl.stream.WriteSession;

/**
 * Grpc based write service.
 */
public class GrpcStreamWriteService extends WriteServiceImplBase {

    private final StreamStore streamStore;
    private final OrderedScheduler scheduler;

    GrpcStreamWriteService(StreamStore streamStore,
                           OrderedScheduler scheduler) {
        this.streamStore = streamStore;
        this.scheduler = scheduler;
    }

    @Override
    public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> responseObserver) {
        return new WriteSession(streamStore, responseObserver, scheduler);
    }
}
