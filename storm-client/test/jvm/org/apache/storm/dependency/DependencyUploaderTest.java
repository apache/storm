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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        File mockFile = mock(File.class);
        when(mockFile.isFile()).thenReturn(true);
        when(mockFile.exists()).thenReturn(true);

        File mockFile2 = mock(File.class);
        when(mockFile.isFile()).thenReturn(true);
        when(mockFile.exists()).thenReturn(false);

        List<File> dependencies = new ArrayList<>();
        dependencies.add(mockFile);
        dependencies.add(mockFile2);

        sut.uploadFiles(dependencies, false);
        fail("Should throw FileNotAvailableException");
    }

    @Test(expected = FileNotAvailableException.class)
    public void uploadFilesWhichOneOfThemIsNotFile() throws Exception {
        File mockFile = mock(File.class);
        when(mockFile.isFile()).thenReturn(true);
        when(mockFile.exists()).thenReturn(true);

        File mockFile2 = mock(File.class);
        when(mockFile.isFile()).thenReturn(false);
        when(mockFile.exists()).thenReturn(true);

        List<File> dependencies = Lists.newArrayList(mockFile, mockFile2);

        sut.uploadFiles(dependencies, false);
        fail("Should throw FileNotAvailableException");
    }

    @Test
    public void uploadFilesWhichOneOfThemIsFailedToBeUploaded() throws Exception {
        File mockFile = createTemporaryDummyFile();

        File mockFile2 = mock(File.class);
        when(mockFile2.getName()).thenReturn("dummy.jar");
        when(mockFile2.isFile()).thenReturn(true);
        when(mockFile2.exists()).thenReturn(true);
        when(mockFile2.getPath()).thenThrow(new RuntimeException("just for test!"));
        when(mockFile2.toPath()).thenThrow(new RuntimeException("just for test!"));

        String mockFileFileNameWithoutExtension = Files.getNameWithoutExtension(mockFile.getName());
        String mockFile2FileNameWithoutExtension = Files.getNameWithoutExtension(mockFile2.getName());

        // we skip uploading first one since we want to test rollback, not upload
        when(mockBlobStore.getBlobMeta(contains(mockFileFileNameWithoutExtension))).thenReturn(new ReadableBlobMeta());
        // we try uploading second one and it should be failed throwing RuntimeException
        when(mockBlobStore.getBlobMeta(contains(mockFile2FileNameWithoutExtension))).thenThrow(new KeyNotFoundException());

        List<File> dependencies = Lists.newArrayList(mockFile, mockFile2);

        try {
            sut.uploadFiles(dependencies, true);
            fail("Should pass RuntimeException");
        } catch (RuntimeException e) {
            // intended behavior
        }

        verify(mockBlobStore).getBlobMeta(contains(mockFileFileNameWithoutExtension));
        verify(mockBlobStore).getBlobMeta(contains(mockFile2FileNameWithoutExtension));

        verify(mockBlobStore).deleteBlob(contains(mockFileFileNameWithoutExtension));
        verify(mockBlobStore, never()).deleteBlob(contains(mockFile2FileNameWithoutExtension));
    }

    @Test
    public void uploadFiles() throws Exception {
        AtomicOutputStream mockOutputStream = mock(AtomicOutputStream.class);
        doNothing().when(mockOutputStream).cancel();

        final AtomicInteger counter = new AtomicInteger();
        final Answer incrementCounter = new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                counter.addAndGet(1);
                return null;
            }
        };

        doAnswer(incrementCounter).when(mockOutputStream).write(anyInt());
        doAnswer(incrementCounter).when(mockOutputStream).write(any(byte[].class));
        doAnswer(incrementCounter).when(mockOutputStream).write(any(byte[].class), anyInt(), anyInt());
        doNothing().when(mockOutputStream).close();

        when(mockBlobStore.getBlobMeta(anyString())).thenThrow(new KeyNotFoundException());
        when(mockBlobStore.createBlob(anyString(), any(SettableBlobMeta.class))).thenReturn(mockOutputStream);

        File mockFile = createTemporaryDummyFile();
        String mockFileFileNameWithoutExtension = Files.getNameWithoutExtension(mockFile.getName());

        List<String> keys = sut.uploadFiles(Lists.newArrayList(mockFile), false);

        assertEquals(1, keys.size());
        assertTrue(keys.get(0).contains(mockFileFileNameWithoutExtension));

        assertTrue(counter.get() > 0);
        verify(mockOutputStream).close();

        ArgumentCaptor<SettableBlobMeta> blobMetaArgumentCaptor = ArgumentCaptor.forClass(SettableBlobMeta.class);
        verify(mockBlobStore).createBlob(anyString(), blobMetaArgumentCaptor.capture());

        SettableBlobMeta actualBlobMeta = blobMetaArgumentCaptor.getValue();
        List<AccessControl> actualAcls = actualBlobMeta.get_acl();
        assertTrue(actualAcls.contains(new AccessControl(AccessControlType.USER,
                                                         BlobStoreAclHandler.READ | BlobStoreAclHandler.WRITE |
                                                         BlobStoreAclHandler.ADMIN)));
        assertTrue(actualAcls.contains(new AccessControl(AccessControlType.OTHER,
                                                         BlobStoreAclHandler.READ)));
    }

    @Test(expected = FileNotAvailableException.class)
    public void uploadArtifactsWhichOneOfThemIsNotFoundInLocal() throws Exception {
        File mockFile = mock(File.class);
        when(mockFile.isFile()).thenReturn(true);
        when(mockFile.exists()).thenReturn(true);

        File mockFile2 = mock(File.class);
        when(mockFile.isFile()).thenReturn(true);
        when(mockFile.exists()).thenReturn(false);

        Map<String, File> artifacts = new LinkedHashMap<>();
        artifacts.put("group:artifact:1.0.0", mockFile);
        artifacts.put("group:artifact:1.1.0", mockFile2);

        sut.uploadArtifacts(artifacts);
        fail("Should throw FileNotAvailableException");
    }

    @Test(expected = FileNotAvailableException.class)
    public void uploadArtifactsWhichOneOfThemIsNotFile() throws Exception {
        File mockFile = mock(File.class);
        when(mockFile.isFile()).thenReturn(true);
        when(mockFile.exists()).thenReturn(true);

        File mockFile2 = mock(File.class);
        when(mockFile.isFile()).thenReturn(false);
        when(mockFile.exists()).thenReturn(true);

        Map<String, File> artifacts = new LinkedHashMap<>();
        artifacts.put("group:artifact:1.0.0", mockFile);
        artifacts.put("group:artifact:1.1.0", mockFile2);

        sut.uploadArtifacts(artifacts);
        fail("Should throw FileNotAvailableException");
    }

    @Test
    public void uploadArtifactsWhichOneOfThemIsFailedToBeUploaded() throws Exception {
        String artifact = "group:artifact:1.0.0";
        String expectedBlobKeyForArtifact = "group-artifact-1.0.0.jar";
        File mockFile = createTemporaryDummyFile();

        String artifact2 = "group:artifact2:2.0.0";
        String expectedBlobKeyForArtifact2 = "group-artifact2-2.0.0.jar";
        File mockFile2 = mock(File.class);
        when(mockFile2.getName()).thenReturn("dummy.jar");
        when(mockFile2.isFile()).thenReturn(true);
        when(mockFile2.exists()).thenReturn(true);
        when(mockFile2.getPath()).thenThrow(new RuntimeException("just for test!"));
        when(mockFile2.toPath()).thenThrow(new RuntimeException("just for test!"));

        // we skip uploading first one since we don't test upload for now
        when(mockBlobStore.getBlobMeta(contains(expectedBlobKeyForArtifact))).thenReturn(new ReadableBlobMeta());
        // we try uploading second one and it should be failed throwing RuntimeException
        when(mockBlobStore.getBlobMeta(contains(expectedBlobKeyForArtifact2))).thenThrow(new KeyNotFoundException());

        Map<String, File> artifacts = new LinkedHashMap<>();
        artifacts.put(artifact, mockFile);
        artifacts.put(artifact2, mockFile2);

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

    @Test
    public void uploadArtifacts() throws Exception {
        AtomicOutputStream mockOutputStream = mock(AtomicOutputStream.class);
        doNothing().when(mockOutputStream).cancel();

        final AtomicInteger counter = new AtomicInteger();
        final Answer incrementCounter = new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                counter.addAndGet(1);
                return null;
            }
        };

        doAnswer(incrementCounter).when(mockOutputStream).write(anyInt());
        doAnswer(incrementCounter).when(mockOutputStream).write(any(byte[].class));
        doAnswer(incrementCounter).when(mockOutputStream).write(any(byte[].class), anyInt(), anyInt());
        doNothing().when(mockOutputStream).close();

        when(mockBlobStore.getBlobMeta(anyString())).thenThrow(new KeyNotFoundException());
        when(mockBlobStore.createBlob(anyString(), any(SettableBlobMeta.class))).thenReturn(mockOutputStream);

        String artifact = "group:artifact:1.0.0";
        String expectedBlobKeyForArtifact = "group-artifact-1.0.0.jar";
        File mockFile = createTemporaryDummyFile();

        Map<String, File> artifacts = new LinkedHashMap<>();
        artifacts.put(artifact, mockFile);
        List<String> keys = sut.uploadArtifacts(artifacts);

        assertEquals(1, keys.size());
        assertTrue(keys.get(0).contains(expectedBlobKeyForArtifact));

        assertTrue(counter.get() > 0);
        verify(mockOutputStream).close();
    }

    private File createTemporaryDummyFile() throws IOException {
        File tempFile = File.createTempFile("tempfile", ".tmp");

        BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));
        bw.write("This is the temporary file content");
        bw.close();

        return tempFile;
    }

}
