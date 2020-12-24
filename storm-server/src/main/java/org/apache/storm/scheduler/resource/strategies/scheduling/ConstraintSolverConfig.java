/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.resource.strategies.scheduling;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component constraint as derived from configuration.
 * This is backward compatible and can parse old style Config.TOPOLOGY_RAS_CONSTRAINTS and Config.TOPOLOGY_SPREAD_COMPONENTS.
 * New style Config.TOPOLOGY_RAS_CONSTRAINTS is map where each component has a list of other incompatible components
 * and an optional number that specifies the maximum co-location count for the component on a node.
 *
 * <p>comp-1 cannot exist on same worker as comp-2 or comp-3, and at most "2" comp-1 on same node</p>
 * <p>comp-2 and comp-4 cannot be on same worker (missing comp-1 is implied from comp-1 constraint)</p>
 *
 *  <p>
 *      { "comp-1": { "maxNodeCoLocationCnt": 2, "incompatibleComponents": ["comp-2", "comp-3" ] },
 *        "comp-2": { "incompatibleComponents": [ "comp-4" ] }
 *      }
 *  </p>
 */
public final class ConstraintSolverConfig {
    private static final Logger LOG = LoggerFactory.getLogger(ConstraintSolverConfig.class);

    public static final String CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT = "maxNodeCoLocationCnt";
    public static final String CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS = "incompatibleComponents";

    /** constraint limiting which components cannot co-exist on same worker.
     * ALL components are represented, some with empty set of components. */
    private Map<String, Set<String>> incompatibleComponentSets = new HashMap<>();
    /** constraint limiting executor instances of component on a node. */
    private Map<String, Integer> maxNodeCoLocationCnts = new HashMap<>();

    private final Map<String, Object> topoConf;
    private final Set<String> comps;
    private final String topoId; // used only in LOG messages

    public ConstraintSolverConfig(TopologyDetails topo) {
        this(topo.getId(), topo.getConf(), new HashSet<>(topo.getExecutorToComponent().values()));
    }

    public ConstraintSolverConfig(String topoId, Map<String, Object> topoConf, Set<String> comps) {
        this.topoId = (topoId == null) ? "<null>" : topoId;
        this.topoConf = Collections.unmodifiableMap(topoConf);
        this.comps = Collections.unmodifiableSet(comps);

        computeComponentConstraints();
    }

    private void computeComponentConstraints() {
        comps.forEach(k -> incompatibleComponentSets.computeIfAbsent(k, x -> new HashSet<>()));
        Object rasConstraints = topoConf.get(Config.TOPOLOGY_RAS_CONSTRAINTS);
        if (rasConstraints == null) {
            LOG.warn("TopoId {}: No config supplied for {}", topoId, Config.TOPOLOGY_RAS_CONSTRAINTS);
        } else if (rasConstraints instanceof List) {
            // old style
            List<List<String>> constraints = (List<List<String>>) rasConstraints;
            for (List<String> constraintPair : constraints) {
                String comp1 = constraintPair.get(0);
                String comp2 = constraintPair.get(1);
                if (!comps.contains(comp1)) {
                    LOG.warn("TopoId {}: Comp {} declared in constraints is not valid!", topoId, comp1);
                    continue;
                }
                if (!comps.contains(comp2)) {
                    LOG.warn("TopoId {}: Comp {} declared in constraints is not valid!", topoId, comp2);
                    continue;
                }
                incompatibleComponentSets.get(comp1).add(comp2);
                incompatibleComponentSets.get(comp2).add(comp1);
            }
        } else {
            Map<String, Map<String, ?>> constraintMap = (Map<String, Map<String, ?>>) rasConstraints;
            constraintMap.forEach((comp1, v) -> {
                if (comps.contains(comp1)) {
                    v.forEach((ctype, constraint) -> {
                        switch (ctype) {
                            case CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT:
                                try {
                                    int numValue = Integer.parseInt("" + constraint);
                                    if (numValue < 1) {
                                        LOG.warn("TopoId {}: {} {} declared for Comp {} is not valid, expected >= 1",
                                                topoId, ctype, numValue, comp1);
                                    } else {
                                        maxNodeCoLocationCnts.put(comp1, numValue);
                                    }
                                } catch (Exception ex) {
                                    LOG.warn("TopoId {}: {} {} declared for Comp {} for topoId {} is not valid, expected >= 1",
                                            topoId, ctype, constraint, comp1);
                                }
                                break;

                            case CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS:
                                if (!(constraint instanceof List || constraint instanceof String)) {
                                    LOG.warn("TopoId {}: {} {} declared for Comp {} is not valid, expecting a list of Comps or 1 Comp",
                                            topoId, ctype, constraint, comp1);
                                    break;
                                }
                                List<String> list;
                                list = (constraint instanceof String) ? Arrays.asList((String) constraint) : (List<String>) constraint;
                                for (String comp2: list) {
                                    if (!comps.contains(comp2)) {
                                        LOG.warn("TopoId {}: {} {} declared for Comp {} is not a valid Comp", topoId, ctype, comp2, comp1);
                                        continue;
                                    }
                                    incompatibleComponentSets.get(comp1).add(comp2);
                                    incompatibleComponentSets.get(comp2).add(comp1);
                                }
                                break;

                            default:
                                LOG.warn("TopoId {}: ConstraintType={} invalid for Comp={}, valid values are {} and {}, ignoring value={}",
                                        topoId, ctype, comp1, CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT,
                                        CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, constraint);
                                break;
                        }
                    });
                } else {
                    LOG.warn("TopoId {}: Component {} is not a valid component", topoId, comp1);
                }
            });
        }

        // process Config.TOPOLOGY_SPREAD_COMPONENTS - old style
        // override only if not defined already using Config.TOPOLOGY_RAS_COMPONENTS above
        Object obj = topoConf.get(Config.TOPOLOGY_SPREAD_COMPONENTS);
        if (obj == null) {
            return;
        }
        if (obj instanceof List) {
            List<String> spread = (List<String>) obj;
            for (String comp : spread) {
                if (!comps.contains(comp)) {
                    LOG.warn("TopoId {}: Invalid Component {} declared in spread {}", topoId, comp, spread);
                    continue;
                }
                if (maxNodeCoLocationCnts.containsKey(comp)) {
                    LOG.warn("TopoId {}: Component {} maxNodeCoLocationCnt={} already defined in {}, ignoring spread config in {}", topoId,
                            comp, maxNodeCoLocationCnts.get(comp), Config.TOPOLOGY_RAS_CONSTRAINTS, Config.TOPOLOGY_SPREAD_COMPONENTS);
                    continue;
                }
                maxNodeCoLocationCnts.put(comp, 1);
            }
        } else {
            LOG.warn("TopoId {}: Ignoring invalid {} config={}", topoId, Config.TOPOLOGY_SPREAD_COMPONENTS, obj);
        }
    }

    /**
     *  Return an object that maps component names to a set of other components
     *  which are incompatible and their executor instances cannot co-exist on the
     *  same worker.
     *  The map will contain entries only for components that have this {@link #CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS}
     *  constraint specified.
     *
     * @return a map of component to a set of components that cannot co-exist on the same worker.
     */
    public Map<String, Set<String>> getIncompatibleComponentSets() {
        return incompatibleComponentSets;
    }

    /**
     *  Return an object that maps component names to a numeric maximum limit of
     *  executor instances (of that component) that can exist on any node.
     *  The map will contain entries only for components that have this {@link #CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT}
     *  constraint specified.
     *
     * @return a map of component to its maximum limit of executor instances on a node.
     */
    public Map<String, Integer> getMaxNodeCoLocationCnts() {
        return maxNodeCoLocationCnts;
    }

}
