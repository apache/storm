package com.alibaba.jstorm.batch.util;

public  enum BatchStatus {
	COMPUTING, 
	PREPARE_COMMIT, 
	COMMIT, 
	REVERT_COMMIT, 
	POST_COMMIT,
	ERROR;

	
	
	public BatchStatus forward() {
		if (this == COMPUTING) {
			return PREPARE_COMMIT;
		}else if (this == PREPARE_COMMIT) {
			return COMMIT;
		}else if (this == COMMIT) {
			return POST_COMMIT;
		}else {
			return null;
		}
	}
	
	public BatchStatus error() {
		if (this == COMMIT) {
			return REVERT_COMMIT;
		}else {
			return ERROR;
		}
	}
	
};
