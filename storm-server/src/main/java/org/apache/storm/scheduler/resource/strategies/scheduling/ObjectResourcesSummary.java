/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource.strategies.scheduling;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;

/**
 * a class to contain individual object resources as well as cumulative stats.
 */
public class ObjectResourcesSummary {
    private List<ObjectResourcesItem> objectResources = new LinkedList<>();
    private final NormalizedResourceOffer availableResourcesOverall;
    private final NormalizedResourceOffer totalResourcesOverall;
    private String identifier;

    public ObjectResourcesSummary(String identifier) {
        this.identifier = identifier;
        this.availableResourcesOverall = new NormalizedResourceOffer();
        this.totalResourcesOverall = new NormalizedResourceOffer();
    }

    public ObjectResourcesSummary(ObjectResourcesSummary other) {
        this(null,
             new NormalizedResourceOffer(other.availableResourcesOverall),
             new NormalizedResourceOffer(other.totalResourcesOverall),
             other.identifier);
        List<ObjectResourcesItem> objectResourcesList = new ArrayList<>();
        other.objectResources
                .forEach(x -> objectResourcesList.add(new ObjectResourcesItem(x)));
        this.objectResources = objectResourcesList;
    }

    public ObjectResourcesSummary(List<ObjectResourcesItem> objectResources, NormalizedResourceOffer availableResourcesOverall,
                                  NormalizedResourceOffer totalResourcesOverall, String identifier) {
        this.objectResources = objectResources;
        this.availableResourcesOverall = availableResourcesOverall;
        this.totalResourcesOverall = totalResourcesOverall;
        this.identifier = identifier;
    }

    public void addObjectResourcesItem(ObjectResourcesItem item) {
        objectResources.add(item);
        availableResourcesOverall.add(item.availableResources);
        totalResourcesOverall.add(item.totalResources);
    }

    public List<ObjectResourcesItem> getObjectResources() {
        return objectResources;
    }

    public NormalizedResourceOffer getAvailableResourcesOverall() {
        return availableResourcesOverall;
    }

    public NormalizedResourceOffer getTotalResourcesOverall() {
        return totalResourcesOverall;
    }
}
