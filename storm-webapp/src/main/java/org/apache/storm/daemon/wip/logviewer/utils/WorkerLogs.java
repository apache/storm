package org.apache.storm.daemon.wip.logviewer.utils;

import org.apache.storm.daemon.DirectoryCleaner;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;
import static org.apache.storm.daemon.utils.ListFunctionalSupport.takeLast;

public class WorkerLogs {

    private WorkerLogs() {
    }

    /**
     * Return the path of the worker log with the format of topoId/port/worker.log.*
     */
    public static String getTopologyPortWorkerLog(File file) {
        try {
            String[] splitted = file.getCanonicalPath().split(Utils.FILE_PATH_SEPARATOR);
            List<String> split = takeLast(Arrays.asList(splitted), 3);

            return String.join(Utils.FILE_PATH_SEPARATOR, split);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<File> getAllLogsForRootDir(File logDir) throws IOException {
        List<File> files = new ArrayList<>();
        Set<File> topoDirFiles = getAllWorkerDirs(logDir);
        if (topoDirFiles != null) {
            for (File portDir : topoDirFiles) {
                files.addAll(DirectoryCleaner.getFilesForDir(portDir));
            }
        }

        return files;
    }

    public static Set<File> getAllWorkerDirs(File rootDir) {
        File[] rootDirFiles = rootDir.listFiles();
        if (rootDirFiles != null) {
            return Arrays.stream(rootDirFiles).flatMap(topoDir -> {
                File[] topoFiles = topoDir.listFiles();
                return topoFiles != null ? Arrays.stream(topoFiles) : Stream.empty();
            }).collect(toCollection(TreeSet::new));
        }

        return new TreeSet<>();
    }

}
