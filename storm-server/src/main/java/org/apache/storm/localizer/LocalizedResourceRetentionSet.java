/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.localizer;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of resources that we can look at to see which ones we retain and which ones should be
 * removed.
 */
public class LocalizedResourceRetentionSet {
    public static final Logger LOG = LoggerFactory.getLogger(LocalizedResourceRetentionSet.class);
    @VisibleForTesting
    final SortedMap<LocallyCachedBlob, Map<String, ? extends LocallyCachedBlob>> noReferences;
    private long currentSize;
    // targetSize in Bytes
    private long targetSize;

    LocalizedResourceRetentionSet(long targetSize) {
        this(targetSize, new LRUComparator());
    }

    LocalizedResourceRetentionSet(long targetSize, Comparator<? super LocallyCachedBlob> cmp) {
        this(targetSize, new TreeMap<>(cmp));
    }

    LocalizedResourceRetentionSet(long targetSize,
                                  SortedMap<LocallyCachedBlob, Map<String, ? extends LocallyCachedBlob>> retain) {
        this.noReferences = retain;
        this.targetSize = targetSize;
    }

    // for testing
    protected int getSizeWithNoReferences() {
        return noReferences.size();
    }

    /**
     * Add blobs to be checked if they can be deleted.
     * @param blobs a map of blob name to the blob object.  The blobs in this map will be deleted from the map
     *     if they are deleted on disk too.
     */
    public void addResources(ConcurrentMap<String, ? extends LocallyCachedBlob> blobs) {
        for (LocallyCachedBlob b : blobs.values()) {
            currentSize += b.getSizeOnDisk();
            if (b.isUsed()) {
                LOG.debug("NOT going to clean up {}, {} depends on it", b.getKey(), b.getDependencies());
                // always retain resources in use
                continue;
            }
            LOG.debug("Possibly going to clean up {} ts {} size {}", b.getKey(), b.getLastUsed(), b.getSizeOnDisk());
            noReferences.put(b, blobs);
        }
    }

    /**
     * Actually cleanup the blobs to try and get below the target cache size.
     * @param store the blobs store client used to check if the blob has been deleted from the blobstore.  If it has, the blob will be
     *     deleted even if the cache is not over the target size.
     * @return a set containing any deleted blobs.
     */
    public Set<LocallyCachedBlob> cleanup(ClientBlobStore store) {
        Set<LocallyCachedBlob> deleted = new HashSet<>();
        LOG.debug("cleanup target size: {} current size is: {}", targetSize, currentSize);
        long bytesOver = currentSize - targetSize;
        //First delete everything that no longer exists...
        for (Iterator<Map.Entry<LocallyCachedBlob, Map<String, ? extends LocallyCachedBlob>>> i = noReferences.entrySet().iterator();
             i.hasNext(); ) {
            Map.Entry<LocallyCachedBlob, Map<String, ? extends LocallyCachedBlob>> rsrc = i.next();
            LocallyCachedBlob resource = rsrc.getKey();
            try {
                if (!store.isRemoteBlobExists(resource.getKey())) {
                    //The key was removed so we should delete it too.
                    Map<String, ? extends LocallyCachedBlob> set = rsrc.getValue();
                    if (removeBlob(resource, set)) {
                        bytesOver -= resource.getSizeOnDisk();
                        LOG.info("Deleted blob: {} (REMOVED FROM CLUSTER).", resource.getKey());
                        deleted.add(resource);
                        i.remove();
                    }
                }
            } catch (AuthorizationException e) {
                //Ignored
            }
        }

        for (Iterator<Map.Entry<LocallyCachedBlob, Map<String, ? extends LocallyCachedBlob>>> i = noReferences.entrySet().iterator();
             bytesOver > 0 && i.hasNext(); ) {
            Map.Entry<LocallyCachedBlob, Map<String, ? extends LocallyCachedBlob>> rsrc = i.next();
            LocallyCachedBlob resource = rsrc.getKey();
            Map<String, ? extends LocallyCachedBlob> set = rsrc.getValue();
            if (removeBlob(resource, set)) {
                bytesOver -= resource.getSizeOnDisk();
                LOG.info("Deleted blob: {} (OVER SIZE LIMIT).", resource.getKey());
                deleted.add(resource);
                i.remove();
            }
        }
        return deleted;
    }

    private boolean removeBlob(LocallyCachedBlob blob, Map<String, ? extends LocallyCachedBlob> blobs) {
        synchronized (blob) {
            if (!blob.isUsed()) {
                try {
                    blob.completelyRemove();
                } catch (Exception e) {
                    LOG.warn("Tried to remove {} but failed with", blob, e);
                }
                blobs.remove(blob.getKey());
                return true;
            }
            return false;
        }
    }

    @Override
    public String toString() {
        return "Cache: " + currentSize;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    static class LRUComparator implements Comparator<LocallyCachedBlob> {
        @Override
        public int compare(LocallyCachedBlob r1, LocallyCachedBlob r2) {
            long ret = r1.getLastUsed() - r2.getLastUsed();
            if (0 == ret) {
                return System.identityHashCode(r1) - System.identityHashCode(r2);
            }
            return ret > 0 ? 1 : -1;
        }
    }
}
