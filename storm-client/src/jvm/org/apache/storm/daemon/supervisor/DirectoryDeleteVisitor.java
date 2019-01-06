/*
 * Copyright 2019 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.supervisor;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryDeleteVisitor extends SimpleFileVisitor<Path> {

    private static final Logger LOG = LoggerFactory.getLogger(DirectoryDeleteVisitor.class);

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        try {
            Files.delete(file);
        } catch (NoSuchFileException e) {
            LOG.debug("Failed to delete file, may have been concurrently deleted", e);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        try {
            Files.delete(dir);
        } catch (NoSuchFileException e) {
            LOG.debug("Failed to delete file, may have been concurrently deleted", e);
        } catch (FileSystemException e) {
            if (!Utils.isOnWindows()) {
                throw e;
            }
            /*
             * Windows normally doesn't allow file deletion if the file has open handles. Files opened with NIO APIs can be deleted without
             * closing all handles, since Java opens them with the FILE_SHARE_DELETE flag set. Deleting the files in this way marks them for
             * deletion.
             *
             * Windows directories cannot be deleted if they contain files marked for deletion, as the files are not removed from the
             * directory listing.
             *
             * It is possible to work around by moving the files out of the directory before deleting, but it has a bunch of unpleasant side
             * effects. e.g. deciding where the files should move to, what happens if they are moved and not deleted.
             *
             * Best we can do is ignore the error until something like POSIX behavior is added to Windows.
             *
             * See
             * https://wpdev.uservoice.com/forums/110705-universal-windows-platform/suggestions/37133743-add-file-disposition-posix-semantics-to-public-api
             */
            if (e.getMessage().contains("The process cannot access the file because it is being used by another process")) {
                //Ignore
            } else {
                throw e;
            }
        }
        return FileVisitResult.CONTINUE;
    }

}
