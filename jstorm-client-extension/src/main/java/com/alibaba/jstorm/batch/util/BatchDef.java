package com.alibaba.jstorm.batch.util;


public class BatchDef {
	public static final String COMPUTING_STREAM_ID = "batch/compute-stream";
	
	public static final String PREPARE_STREAM_ID = "batch/parepare-stream";

	public static final String COMMIT_STREAM_ID = "batch/commit-stream";

	public static final String REVERT_STREAM_ID = "batch/revert-stream";
	
	public static final String POST_STREAM_ID = "batch/post-stream";

	public static final String SPOUT_TRIGGER = "spout_trigger";

	public static final String BATCH_ZK_ROOT = "batch";

	public static final String ZK_COMMIT_DIR = "/commit";

	public static final int ZK_COMMIT_RESERVER_NUM = 3;

	public static final String ZK_SEPERATOR = "/";

	
}
