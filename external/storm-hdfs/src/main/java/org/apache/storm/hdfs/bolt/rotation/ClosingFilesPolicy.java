package org.apache.storm.hdfs.bolt.rotation;

import java.io.Serializable;

/**
 * New Interface lets you define when particular file should be closed.
 */
public interface ClosingFilesPolicy extends Serializable {

    public boolean closeWriter();

    public void reset();

    public ClosingFilesPolicy copy();
}
