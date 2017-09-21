/**
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

import com.google.common.annotations.VisibleForTesting;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A set of resources that we can look at to see which ones we retain and which ones should be
 * removed.
 */
public class LocalizedResourceRetentionSet {
    public static final Logger LOG = LoggerFactory.getLogger(LocalizedResourceRetentionSet.class);
    private long delSize;
    private long currentSize;
    // targetSize in Bytes
    private long targetSize;
    @VisibleForTesting
    final SortedMap<ComparableResource, CleanableResourceSet> noReferences;
    private int resourceCount = 0;

    LocalizedResourceRetentionSet(long targetSize) {
        this(targetSize, new LRUComparator());
    }

    LocalizedResourceRetentionSet(long targetSize, Comparator<? super ComparableResource> cmp) {
        this(targetSize, new TreeMap<>(cmp));
    }

    LocalizedResourceRetentionSet(long targetSize,
                                  SortedMap<ComparableResource, CleanableResourceSet> retain) {
        this.noReferences = retain;
        this.targetSize = targetSize;
    }

    // for testing
    protected int getSizeWithNoReferences() {
        return noReferences.size();
    }

    protected void addResourcesForSet(Iterator<LocalizedResource> setIter, LocalizedResourceSet set) {
        CleanableLocalizedResourceSet cleanset = new CleanableLocalizedResourceSet(set);
        for (Iterator<LocalizedResource> iter = setIter; setIter.hasNext(); ) {
            LocalizedResource lrsrc = iter.next();
            currentSize += lrsrc.getSize();
            resourceCount ++;
            if (lrsrc.getRefCount() > 0) {
                // always retain resources in use
                continue;
            }
            noReferences.put(new LocalizedBlobComparableResource(lrsrc), cleanset);
        }
    }

    public void addResources(LocalizedResourceSet set) {
        addResourcesForSet(set.getLocalFilesIterator(), set);
        addResourcesForSet(set.getLocalArchivesIterator(), set);
    }

    public void addResources(ConcurrentHashMap<String, LocallyCachedBlob> blobs) {
        CleanableLocalizedLocallyCachedBlob set = new CleanableLocalizedLocallyCachedBlob(blobs);
        for (LocallyCachedBlob b: blobs.values()) {
            currentSize += b.getSizeOnDisk();
            resourceCount ++;
            if (b.isUsed()) {
                // always retain resources in use
                continue;
            }
            LocallyCachedBlobComparableResource cb = new LocallyCachedBlobComparableResource(b);
            noReferences.put(cb, set);
        }
    }

    public void cleanup() {
        LOG.debug("cleanup target size: {} current size is: {}", targetSize, currentSize);
        for (Iterator<Map.Entry<ComparableResource, CleanableResourceSet>> i =
             noReferences.entrySet().iterator();
             currentSize - delSize > targetSize && i.hasNext();) {
            Map.Entry<ComparableResource, CleanableResourceSet> rsrc = i.next();
            ComparableResource resource = rsrc.getKey();
            CleanableResourceSet set = rsrc.getValue();
            if (resource != null && set.remove(resource)) {
                if (set.deleteUnderlyingResource(resource)) {
                    delSize += resource.getSize();
                    LOG.info("deleting: {} with size of: {}", resource.getNameForDebug(), resource.getSize());
                    i.remove();
                } else {
                    // since it failed to delete add it back so it gets retried
                    set.add(resource.getKey(), resource);
                }
            }
        }
    }

    @VisibleForTesting
    public boolean deleteResource(CleanableResourceSet set, ComparableResource resource) {
        return set.deleteUnderlyingResource(resource);
    }

    public long getCurrentSize() {
        return currentSize;
    }

    public int getResourceCount() {
        return resourceCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Cache: ").append(currentSize).append(", ");
        sb.append("Deleted: ").append(delSize);
        return sb.toString();
    }

    interface ComparableResource {
        long getLastAccessTime();

        long getSize();

        String getNameForDebug();

        String getKey();
    }

    interface CleanableResourceSet {
        boolean remove(ComparableResource resource);

        void add(String key, ComparableResource resource);

        boolean deleteUnderlyingResource(ComparableResource resource);
    }

    public static class LocallyCachedBlobComparableResource implements ComparableResource {
        private final LocallyCachedBlob blob;

        public LocallyCachedBlobComparableResource(LocallyCachedBlob blob) {
            this.blob = blob;
        }

        @Override
        public long getLastAccessTime() {
            return blob.getLastUsed();
        }

        @Override
        public long getSize() {
            return blob.getSizeOnDisk();
        }

        @Override
        public String getNameForDebug() {
            return blob.getKey();
        }

        @Override
        public String getKey() {
            return blob.getKey();
        }

        @Override
        public String toString() {
            return blob.toString();
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof LocallyCachedBlobComparableResource) {
                return blob.equals(((LocallyCachedBlobComparableResource) other).blob);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return blob.hashCode();
        }
    }

    private static class CleanableLocalizedLocallyCachedBlob implements CleanableResourceSet {
        private final ConcurrentHashMap<String, LocallyCachedBlob> blobs;

        public CleanableLocalizedLocallyCachedBlob(ConcurrentHashMap<String, LocallyCachedBlob> blobs) {
            this.blobs = blobs;
        }

        @Override
        public boolean remove(ComparableResource resource) {
            if (!(resource instanceof LocallyCachedBlobComparableResource)) {
                throw new IllegalStateException(resource + " must be a LocallyCachedBlobComparableResource");
            }
            LocallyCachedBlob blob = ((LocallyCachedBlobComparableResource)resource).blob;
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
        public void add(String key, ComparableResource resource) {
            ///NOOP not used
        }

        @Override
        public boolean deleteUnderlyingResource(ComparableResource resource) {
            //NOOP not used
            return true;
        }
    }

    private static class LocalizedBlobComparableResource implements ComparableResource {
        private final LocalizedResource resource;

        private LocalizedBlobComparableResource(LocalizedResource resource) {
            this.resource = resource;
        }

        @Override
        public long getLastAccessTime() {
            return resource.getLastAccessTime();
        }

        @Override
        public long getSize() {
            return resource.getSize();
        }

        @Override
        public String getNameForDebug() {
            return resource.getFilePath();
        }

        @Override
        public String getKey() {
            return resource.getKey();
        }

        @Override
        public String toString() {
            return resource.getKey() + " at " + resource.getFilePathWithVersion();
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof LocalizedBlobComparableResource) {
                return resource.equals(((LocalizedBlobComparableResource) other).resource);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return resource.hashCode();
        }
    }

    private static class CleanableLocalizedResourceSet implements CleanableResourceSet {
        private final LocalizedResourceSet set;

        public CleanableLocalizedResourceSet(LocalizedResourceSet set) {
            this.set = set;
        }

        @Override
        public boolean remove(ComparableResource resource) {
            if (!(resource instanceof LocalizedBlobComparableResource)) {
                throw new IllegalStateException(resource + " must be a LocalizedBlobComparableResource");
            }
            return set.remove(((LocalizedBlobComparableResource)resource).resource);
        }

        @Override
        public void add(String key, ComparableResource resource) {
            if (!(resource instanceof LocalizedBlobComparableResource)) {
                throw new IllegalStateException(resource + " must be a LocalizedBlobComparableResource");
            }
            LocalizedResource r = ((LocalizedBlobComparableResource)resource).resource;
            set.add(key, r, r.isUncompressed());
        }

        @Override
        public boolean deleteUnderlyingResource(ComparableResource resource) {
            if (resource instanceof LocalizedBlobComparableResource) {
                LocalizedResource lr = ((LocalizedBlobComparableResource) resource).resource;
                try {
                    Path fileWithVersion = new File(lr.getFilePathWithVersion()).toPath();
                    Path currentSymLink = new File(lr.getCurrentSymlinkPath()).toPath();
                    Path versionFile = new File(lr.getVersionFilePath()).toPath();

                    if (lr.isUncompressed()) {
                        if (Files.exists(fileWithVersion)) {
                            // this doesn't follow symlinks, which is what we want
                            FileUtils.deleteDirectory(fileWithVersion.toFile());
                        }
                    } else {
                        Files.deleteIfExists(fileWithVersion);
                    }
                    Files.deleteIfExists(currentSymLink);
                    Files.deleteIfExists(versionFile);
                    return true;
                } catch (IOException e) {
                    LOG.warn("Could not delete: {}", resource.getNameForDebug(), e);
                }
                return false;
            }  else {
                throw new IllegalArgumentException("Don't know how to handle a " + resource.getClass());
            }
        }
    }

    static class LRUComparator implements Comparator<ComparableResource> {
        public int compare(ComparableResource r1, ComparableResource r2) {
            long ret = r1.getLastAccessTime() - r2.getLastAccessTime();
            if (0 == ret) {
                return System.identityHashCode(r1) - System.identityHashCode(r2);
            }
            return ret > 0 ? 1 : -1;
        }
    }
}
