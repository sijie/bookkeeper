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

package org.apache.bookkeeper.stream.server.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.HandlerRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.grpc.proxy.ProxyHandlerRegistry;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.server.conf.StorageServerConfiguration;
import org.apache.bookkeeper.stream.server.exceptions.StorageServerRuntimeException;
import org.apache.bookkeeper.stream.storage.StorageResources;
import org.apache.bookkeeper.stream.storage.api.StorageContainerStore;
import org.apache.bookkeeper.stream.storage.impl.grpc.GrpcServices;

/**
 * KeyRange Server.
 */
@Slf4j
public class GrpcServer extends AbstractLifecycleComponent<StorageServerConfiguration> {

    public static GrpcServer build(GrpcServerSpec spec) {
        return new GrpcServer(
            spec.storeSupplier().get(),
            spec.storeServerConf(),
            spec.storageResources(),
            spec.endpoint(),
            spec.localServerName(),
            spec.localHandlerRegistry(),
            spec.statsLogger());
    }

    private final Endpoint myEndpoint;
    private final Server grpcServer;
    private final Resource<OrderedScheduler> schedulerResource;
    private final OrderedScheduler scheduler;

    @VisibleForTesting
    public GrpcServer(StorageContainerStore storageContainerStore,
                      StorageServerConfiguration conf,
                      StorageResources resources,
                      Endpoint myEndpoint,
                      String localServerName,
                      HandlerRegistry localHandlerRegistry,
                      StatsLogger statsLogger) {
        super("range-grpc-server", conf, statsLogger);
        this.myEndpoint = myEndpoint;
        this.schedulerResource = resources.scheduler();
        this.scheduler = SharedResourceManager.shared().get(schedulerResource);
        if (null != localServerName) {
            InProcessServerBuilder serverBuilder = InProcessServerBuilder
                .forName(localServerName)
                .directExecutor();
            if (null != localHandlerRegistry) {
                serverBuilder = serverBuilder.fallbackHandlerRegistry(localHandlerRegistry);
            }
            this.grpcServer = serverBuilder.build();
        } else {
            ProxyHandlerRegistry.Builder proxyRegistryBuilder = ProxyHandlerRegistry.newBuilder()
                .setChannelFinder(storageContainerStore);
            for (ServerServiceDefinition definition : GrpcServices.create(null, scheduler)) {
                proxyRegistryBuilder = proxyRegistryBuilder.addService(definition);
            }
            this.grpcServer = ServerBuilder.forPort(this.myEndpoint.getPort())
                .addService(new GrpcStorageContainerService(storageContainerStore))
                .fallbackHandlerRegistry(proxyRegistryBuilder.build())
                .build();
        }
    }

    @VisibleForTesting
    Server getGrpcServer() {
        return grpcServer;
    }

    @Override
    protected void doStart() {
        try {
            grpcServer.start();
        } catch (IOException e) {
            log.error("Failed to start grpc server", e);
            throw new StorageServerRuntimeException("Failed to start grpc server", e);
        }
    }

    @Override
    protected void doStop() {
        grpcServer.shutdown();
    }

    @Override
    protected void doClose() {
        SharedResourceManager.shared().release(schedulerResource, scheduler);
    }

}
