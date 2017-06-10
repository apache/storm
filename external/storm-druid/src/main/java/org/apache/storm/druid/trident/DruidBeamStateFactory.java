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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.druid.trident;

import org.apache.storm.druid.bolt.DruidBeamFactory;
import org.apache.storm.druid.bolt.ITupleDruidEventMapper;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

public class DruidBeamStateFactory<E> implements StateFactory {
    DruidBeamFactory beamFactory = null;
    ITupleDruidEventMapper druidEventMapper = null;

    public DruidBeamStateFactory(DruidBeamFactory<E> beamFactory,  ITupleDruidEventMapper<E> druidEventMapper) {
        this.beamFactory = beamFactory;
        this.druidEventMapper = druidEventMapper;
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new DruidBeamState(beamFactory.makeBeam(conf , metrics), druidEventMapper);
    }
}
