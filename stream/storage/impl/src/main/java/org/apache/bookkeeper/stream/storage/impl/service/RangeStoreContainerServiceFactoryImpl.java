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

package org.apache.bookkeeper.stream.storage.impl.service;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerService;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerServiceFactory;
import org.apache.bookkeeper.stream.storage.api.service.RangeStoreServiceFactory;

/**
 * The default storage container factory for creating {@link StorageContainer}s.
 */
public class RangeStoreContainerServiceFactoryImpl implements StorageContainerServiceFactory {

    private final RangeStoreServiceFactory serviceFactory;
    private final Resource<OrderedScheduler> schedulerResource;
    private final OrderedScheduler scheduler;

    public RangeStoreContainerServiceFactoryImpl(RangeStoreServiceFactory serviceFactory,
                                                 Resource<OrderedScheduler> schedulerResource) {
        this.schedulerResource = schedulerResource;
        this.scheduler = SharedResourceManager.shared().get(schedulerResource);
        this.serviceFactory = serviceFactory;
    }

    @Override
    public StorageContainerService createStorageContainerService(long scId) {
        return new RangeStoreContainerServiceImpl(serviceFactory.createService(scId), scheduler);
    }

    @Override
    public void close() {
        serviceFactory.close();
        SharedResourceManager.shared().release(schedulerResource, scheduler);
    }
}
