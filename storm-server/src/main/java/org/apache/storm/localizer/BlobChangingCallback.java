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

package org.apache.storm.localizer;

import org.apache.storm.generated.LocalAssignment;

/**
 * Callback for when a localized blob is going to change.
 */
public interface BlobChangingCallback {

    /**
     * Informs the listener that a blob has changed and is ready to update and replace a localized blob that has been marked as tied to the
     * life cycle of the worker process.
     *
     * <p>If `go.getLatch()` is never called before the method completes it is assumed that the listener is good with the blob changing.
     *
     * @param assignment the assignment this resource and callback are registered with.
     * @param port       the port that this resource and callback are registered with.
     * @param blob       the blob that is going to change.
     * @param go         a way to indicate if the listener is ready for the resource to change.
     */
    void blobChanging(LocalAssignment assignment, int port, LocallyCachedBlob blob, GoodToGo go);
}
