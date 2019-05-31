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

package org.apache.storm.dependency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.shade.com.google.common.io.MoreFiles;
import org.apache.storm.testing.TmpPath;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class DependencyUploaderTest {

    private DependencyUploader sut;
    private ClientBlobStore mockBlobStore;

    @Before
    public void setUp() throws Exception {
        sut = new DependencyUploader();
        mockBlobStore = mock(ClientBlobStore.class);
        sut.setBlobStore(mockBlobStore);
        doNothing().when(mockBlobStore).shutdown();
    }

    @After
    public void tearDown() throws Exception {
        sut.shutdown();
    }

    @Test(expected = FileNotAvailableException.class)
    public void uploadFilesWhichOneOfThemIsNotFoundInLocal() throws Exception {
        try (TmpPath existingFile = new TmpPath()) {
            writeDummyContent(existingFile.getPath());
            Path missingFile = Paths.get(TmpPath.localTempPath());

            sut.uploadFiles(Arrays.asList(existingFile.getPath(), missingFile), false);
            fail("Should throw FileNotAvailableException");
        }
    }

    @Test(expected = FileNotAvailableException.class)
    public void uploadFilesWhichOneOfThemIsNotFile() throws Exception {
        try (TmpPath existingFile = new TmpPath()) {
            writeDummyContent(existingFile.getPath());
            Path dir = Paths.get(TmpPath.localTempPath());
            Files.createDirectory(dir);

            sut.uploadFiles(Arrays.asList(existingFile.getPath(), dir), false);
            fail("Should throw FileNotAvailableException");
        }
    }

    @Test
    public void uploadFilesWhichOneOfThemIsFailedToBeUploaded() throws Exception {
        try (TmpPath existingFile = new TmpPath(); TmpPath existingFile2 = new TmpPath()) {
            Path path1 = existingFile.getPath();
            Path path2 = existingFile2.getPath().resolve("dummy.jar");
            writeDummyContent(path1);
            writeDummyContent(path2);

            String path1NameWithoutExtension = MoreFiles.getNameWithoutExtension(path1);
            String path2NameWithoutExtension = MoreFiles.getNameWithoutExtension(path2);

            // we skip uploading first one since we want to test rollback, not upload
            when(mockBlobStore.getBlobMeta(contains(path1NameWithoutExtension))).thenReturn(new ReadableBlobMeta());
            // we try uploading second one and it should be failed throwing RuntimeException
            when(mockBlobStore.getBlobMeta(contains(path2NameWithoutExtension))).thenThrow(new KeyNotFoundException());

            try {
                sut.uploadFiles(Arrays.asList(path1, path2), true);
                fail("Should pass RuntimeException");
            } catch (RuntimeException e) {
                // intended behavior
            }

            verify(mockBlobStore).getBlobMeta(contains(path1NameWithoutExtension));
            verify(mockBlobStore).getBlobMeta(contains(path2NameWithoutExtension));

            verify(mockBlobStore).deleteBlob(contains(path1NameWithoutExtension));
            verify(mockBlobStore, never()).deleteBlob(contains(path2NameWithoutExtension));
        }
    }

    @Test
    public void uploadFiles() throws Exception {
        try (TmpPath existingFile = new TmpPath()) {
            Path path = existingFile.getPath();
            writeDummyContent(path);
            AtomicBoolean hasWritten = new AtomicBoolean(false);
            AtomicBoolean isClosed = new AtomicBoolean(false);
            AtomicOutputStream mockOutputStream = new AtomicOutputStream() {
                @Override
                public void cancel() throws IOException {
                }

                @Override
                public void write(int b) throws IOException {
                    hasWritten.set(true);
                }

                @Override
                public void close() throws IOException {
                    isClosed.set(true);
                }
            };
            when(mockBlobStore.getBlobMeta(anyString())).thenThrow(new KeyNotFoundException());
            when(mockBlobStore.createBlob(anyString(), any(SettableBlobMeta.class))).thenReturn(mockOutputStream);

            List<String> keys = sut.uploadFiles(Arrays.asList(path), false);

            assertEquals(1, keys.size());
            assertTrue(keys.get(0).contains(MoreFiles.getNameWithoutExtension(path)));

            assertTrue(hasWritten.get());
            assertTrue(isClosed.get());

            ArgumentCaptor<SettableBlobMeta> blobMetaArgumentCaptor = ArgumentCaptor.forClass(SettableBlobMeta.class);
            verify(mockBlobStore).createBlob(anyString(), blobMetaArgumentCaptor.capture());

            SettableBlobMeta actualBlobMeta = blobMetaArgumentCaptor.getValue();
            List<AccessControl> actualAcls = actualBlobMeta.get_acl();
            assertTrue(actualAcls.contains(new AccessControl(AccessControlType.USER,
                BlobStoreAclHandler.READ
                | BlobStoreAclHandler.WRITE
                | BlobStoreAclHandler.ADMIN)));
            assertTrue(actualAcls.contains(new AccessControl(AccessControlType.OTHER,
                BlobStoreAclHandler.READ)));
        }
    }

    @Test(expected = FileNotAvailableException.class)
    public void uploadArtifactsWhichOneOfThemIsNotFoundInLocal() throws Exception {
        try (TmpPath existingFile = new TmpPath()) {
            writeDummyContent(existingFile.getPath());
            Path missingFile = Paths.get(TmpPath.localTempPath());

            Map<String, Path> artifacts = new LinkedHashMap<>();
            artifacts.put("group:artifact:1.0.0", existingFile.getPath());
            artifacts.put("group:artifact:1.1.0", missingFile);

            sut.uploadArtifacts(artifacts);
            fail("Should throw FileNotAvailableException");
        }
    }

    @Test(expected = FileNotAvailableException.class)
    public void uploadArtifactsWhichOneOfThemIsNotFile() throws Exception {
        try (TmpPath existingFile = new TmpPath()) {
            writeDummyContent(existingFile.getPath());
            Path dir = Paths.get(TmpPath.localTempPath());
            Files.createDirectory(dir);
            Map<String, Path> artifacts = new LinkedHashMap<>();
            artifacts.put("group:artifact:1.0.0", existingFile.getPath());
            artifacts.put("group:artifact:1.1.0", dir);

            sut.uploadArtifacts(artifacts);
            fail("Should throw FileNotAvailableException");
        }
    }

    @Test
    public void uploadArtifactsWhichOneOfThemIsFailedToBeUploaded() throws Exception {
        try (TmpPath existingFile = new TmpPath(); TmpPath existingFile2 = new TmpPath()) {
            Path path1 = existingFile.getPath();
            String artifact = "group:artifact:1.0.0";
            String expectedBlobKeyForArtifact = "group-artifact-1.0.0.jar";
            Path path2 = existingFile2.getPath().resolve("dummy.jar");
            String artifact2 = "group:artifact2:2.0.0";
            String expectedBlobKeyForArtifact2 = "group-artifact2-2.0.0.jar";
            writeDummyContent(path1);
            writeDummyContent(path2);

            // we skip uploading first one since we want to test rollback, not upload
            when(mockBlobStore.getBlobMeta(contains(expectedBlobKeyForArtifact))).thenReturn(new ReadableBlobMeta());
            // we try uploading second one and it should be failed throwing RuntimeException
            when(mockBlobStore.getBlobMeta(contains(expectedBlobKeyForArtifact2))).thenThrow(new KeyNotFoundException());

            Map<String, Path> artifacts = new LinkedHashMap<>();
            artifacts.put(artifact, path2);
            artifacts.put(artifact2, path2);

            try {
                sut.uploadArtifacts(artifacts);
                fail("Should pass RuntimeException");
            } catch (RuntimeException e) {
                // intended behavior
            }

            verify(mockBlobStore).getBlobMeta(contains(expectedBlobKeyForArtifact));
            verify(mockBlobStore).getBlobMeta(contains(expectedBlobKeyForArtifact2));

            // never rollback
            verify(mockBlobStore, never()).deleteBlob(contains(expectedBlobKeyForArtifact));
            verify(mockBlobStore, never()).deleteBlob(contains(expectedBlobKeyForArtifact2));
        }
    }

    @Test
    public void uploadArtifacts() throws Exception {
        try (TmpPath existingFile = new TmpPath()) {
            Path path = existingFile.getPath();
            String artifact = "group:artifact:1.0.0";
            String expectedBlobKeyForArtifact = "group-artifact-1.0.0.jar";
            writeDummyContent(path);
            AtomicBoolean hasWritten = new AtomicBoolean(false);
            AtomicBoolean isClosed = new AtomicBoolean(false);
            AtomicOutputStream mockOutputStream = new AtomicOutputStream() {
                @Override
                public void cancel() throws IOException {
                }

                @Override
                public void write(int b) throws IOException {
                    hasWritten.set(true);
                }

                @Override
                public void close() throws IOException {
                    isClosed.set(true);
                }
            };
            when(mockBlobStore.getBlobMeta(anyString())).thenThrow(new KeyNotFoundException());
            when(mockBlobStore.createBlob(anyString(), any(SettableBlobMeta.class))).thenReturn(mockOutputStream);

            List<String> keys = sut.uploadArtifacts(Collections.singletonMap(artifact, path));

            assertEquals(1, keys.size());
            assertTrue(keys.get(0).contains(expectedBlobKeyForArtifact));

            assertTrue(hasWritten.get());
            assertTrue(isClosed.get());
        }
    }

    private void writeDummyContent(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        Files.write(path, ("This is test content from " + getClass().getSimpleName()).getBytes());
    }

}
