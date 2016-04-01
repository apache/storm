package org.apache.storm.utils;

import org.apache.commons.io.FileUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class Zipper {

    public static void zip(List<String> paths, String zipFilePath) throws IOException {
        File zipFile = new File(zipFilePath);
        if (!zipFile.exists()) {
            zipFile.delete();
        }
        zipFile.createNewFile();

        ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(zipFile));
        Map<String, File> zipEntryFiles = new HashMap<>();
        for (String path : paths) {
            File file = new File(path);
            String parentPath = file.getParent();
            if (file.isDirectory()) {
                Collection<File> nestedFiles = FileUtils.listFiles(file, null, true);
                for (File nestedFile : nestedFiles) {
                    String relativePath = Paths.get(parentPath).relativize(Paths.get(nestedFile.getPath())).toString();
                    zipEntryFiles.put(relativePath, nestedFile);
                }
            } else {
                zipEntryFiles.put(file.getName(), file);
            }
        }
        for (Map.Entry<String, File> zipEntryFile : zipEntryFiles.entrySet()) {
            File fileToBeZipped = zipEntryFile.getValue();
            zipOutputStream.putNextEntry(new ZipEntry(zipEntryFile.getKey()));
            copyContents(zipOutputStream, new FileInputStream(fileToBeZipped));
            zipOutputStream.closeEntry();
        }
        zipOutputStream.close();
    }

    public static void unzip(String zipFile, String localDir) throws IOException {
        File file = new File(localDir);
        if (!file.exists()) {
            file.mkdir();
        }

        ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFile));
        ZipEntry entry = zipInputStream.getNextEntry();
        while (null != entry) {
            File outputFile = new File(localDir, entry.getName());
            outputFile.getParentFile().mkdirs();
            FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
            copyContents(fileOutputStream, zipInputStream);
            zipInputStream.closeEntry();
            entry = zipInputStream.getNextEntry();
        }
        zipInputStream.close();
    }

    private static void copyContents(OutputStream out, InputStream inputStream) throws IOException {
        BufferedInputStream in = new BufferedInputStream(inputStream);
        byte[] buffer = new byte[1024];
        int len;
        while ((len = in.read(buffer)) >= 0) {
            out.write(buffer, 0, len);
        }
    }

}
