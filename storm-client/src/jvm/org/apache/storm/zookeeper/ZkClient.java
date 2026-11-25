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

package org.apache.storm.zookeeper;

import java.util.List;
import org.apache.storm.cluster.ClusterStateListener;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Stat;

/**
 * A Storm-owned handle to a started ZooKeeper session.
 *
 * <p>This is the type Storm uses in its public and protected APIs wherever a ZooKeeper client is
 * passed around. It deliberately exposes only the operations Storm itself needs, and each method is
 * named after the native ZooKeeper call it maps to, so the implementation can be swapped without
 * touching callers. Instances are obtained from
 * {@link ClientZookeeper#mkClient(java.util.Map, List, Object, String,
 * org.apache.storm.callback.WatcherCallBack, java.util.Map, org.apache.storm.cluster.DaemonType)}.
 *
 * <p>Contract that every implementation must honour:
 * <ol>
 *     <li><b>Paths</b> are absolute and relative to the chroot the client was created with. The
 *     client does not normalize them; callers such as {@link ClientZookeeper} do.</li>
 *     <li><b>Exceptions</b>: ZooKeeper {@link KeeperException} subclasses (for example
 *     {@link KeeperException.NoNodeException} and {@link KeeperException.NodeExistsException})
 *     propagate unwrapped, so callers may catch them directly.</li>
 *     <li><b>Watches</b>: {@code watch == true} registers a one-shot ZooKeeper watch whose events
 *     are delivered to the {@link org.apache.storm.callback.WatcherCallBack} the client was created
 *     with. If the client was created without a callback, events are dropped.</li>
 *     <li><b>ACLs</b>: {@code acls == null} means the client's default ACL, which is the
 *     {@link org.apache.storm.cluster.DaemonType} default ACL given at creation, or
 *     {@code OPEN_ACL_UNSAFE} when that is {@code null}. Parent nodes created because
 *     {@code createParents == true} always get the client's default ACL, never the ACL passed to
 *     the call.</li>
 *     <li><b>Retries</b>: operations block until the session is connected and are retried on
 *     connection loss according to the configured
 *     {@link org.apache.storm.utils.StormBoundedExponentialBackoffRetry} parameters
 *     ({@code storm.zookeeper.retry.interval},
 *     {@code storm.zookeeper.retry.intervalceiling.millis},
 *     {@code storm.zookeeper.retry.times}).</li>
 *     <li><b>Sync</b>: {@link #sync(String)} is asynchronous (fire-and-forget) and does not wait
 *     for the sync to complete.</li>
 *     <li><b>Default data</b>: {@link #createWithDefaultData(String, CreateMode, List, boolean)}
 *     writes the client's default payload, which is the bytes of
 *     {@code InetAddress.getLocalHost().getHostAddress()}. This is distinct from
 *     {@link #create(String, byte[], CreateMode, List, boolean)} with {@code data == null}, which
 *     stores no data.</li>
 * </ol>
 */
public interface ZkClient extends AutoCloseable {

    /**
     * Create a node.
     *
     * @param path absolute path of the node to create
     * @param data the data to store, may be {@code null} for no data
     * @param mode the create mode
     * @param acls the ACL for the node, or {@code null} for the client default ACL
     * @param createParents whether missing parent nodes are created (with the client default ACL)
     * @return the actual path of the created node (differs from {@code path} for sequential modes)
     * @throws Exception on failure, ZooKeeper exceptions unwrapped
     */
    String create(String path, byte[] data, CreateMode mode, List<ACL> acls, boolean createParents)
        throws Exception;

    /**
     * Create a node storing the client's default payload (the local host address bytes).
     *
     * @param path absolute path of the node to create
     * @param mode the create mode
     * @param acls the ACL for the node, or {@code null} for the client default ACL
     * @param createParents whether missing parent nodes are created (with the client default ACL)
     * @return the actual path of the created node
     * @throws Exception on failure, ZooKeeper exceptions unwrapped
     */
    String createWithDefaultData(String path, CreateMode mode, List<ACL> acls,
        boolean createParents) throws Exception;

    /**
     * Check whether a node exists.
     *
     * @param path absolute path
     * @param watch whether to leave a watch on the node (set even when the node is absent)
     * @return the node's stat, or {@code null} if the node does not exist
     * @throws Exception on failure, ZooKeeper exceptions unwrapped
     */
    Stat exists(String path, boolean watch) throws Exception;

    /**
     * Read the data of a node.
     *
     * @param path absolute path
     * @param watch whether to leave a watch on the node
     * @param stat if not {@code null}, filled with the node's stat
     * @return the node data
     * @throws Exception on failure, for example {@link KeeperException.NoNodeException}
     */
    byte[] getData(String path, boolean watch, Stat stat) throws Exception;

    /**
     * Unconditionally (any version) set the data of a node.
     *
     * @param path absolute path
     * @param data new data
     * @return the node's new stat
     * @throws Exception on failure, ZooKeeper exceptions unwrapped
     */
    Stat setData(String path, byte[] data) throws Exception;

    /**
     * List the children of a node.
     *
     * @param path absolute path
     * @param watch whether to leave a watch on the node
     * @return the child names (not full paths)
     * @throws Exception on failure, ZooKeeper exceptions unwrapped
     */
    List<String> getChildren(String path, boolean watch) throws Exception;

    /**
     * Unconditionally (any version) delete a node.
     *
     * @param path absolute path
     * @param recursive whether to delete the node's children first
     * @throws Exception on failure, ZooKeeper exceptions unwrapped
     */
    void delete(String path, boolean recursive) throws Exception;

    /**
     * Read the ACL of a node.
     *
     * @param path absolute path
     * @return the node's ACL
     * @throws Exception on failure, ZooKeeper exceptions unwrapped
     */
    List<ACL> getAcl(String path) throws Exception;

    /**
     * Unconditionally (any version) set the ACL of a node.
     *
     * @param path absolute path
     * @param acls the new ACL
     * @throws Exception on failure, ZooKeeper exceptions unwrapped
     */
    void setAcl(String path, List<ACL> acls) throws Exception;

    /**
     * Asynchronously ask the server to sync the given path with the leader. This call does not
     * wait for the sync to complete.
     *
     * @param path absolute path
     * @throws Exception if the request could not be issued
     */
    void sync(String path) throws Exception;

    /**
     * Register a listener for connection state changes of this client.
     *
     * @param listener the listener to notify
     */
    void addConnectionStateListener(ClusterStateListener listener);

    /**
     * Close the session and release all resources.
     */
    @Override
    void close();
}
