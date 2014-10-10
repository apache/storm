package com.alibaba.jstorm.batch.meta;

import java.io.Serializable;
import java.util.Properties;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Meta Spout Setting
 * 
 * All needed configs must prepare before submit topopoly
 * 
 * @author longda
 */
public class MetaSpoutConfig implements Serializable {

	private static final long serialVersionUID = 4157424979688593280L;

	private final String consumerGroup;

	/**
	 * Alipay need set nameServer, taobao don't need set this field
	 */
	private final String nameServer;

	private final String topic;

	private final String subExpress;

	/**
	 * The max allowed failures for one single message, skip the failure message
	 * if excesses
	 * 
	 * -1 means try again until success
	 */
	private int maxFailTimes = DEFAULT_FAIL_TIME;
	public static final int DEFAULT_FAIL_TIME = 10;

	/**
	 * One batch's message number
	 * 
	 */
	private int batchMsgNum = DEFAULT_BATCH_MSG_NUM;
	public static final int DEFAULT_BATCH_MSG_NUM = 256;

	/**
	 * Consumer start time Null means start from the last consumption
	 * time(CONSUME_FROM_LAST_OFFSET)
	 * 
	 */
	private Long startTimeStamp;

	private Properties peroperties;

	public MetaSpoutConfig(String consumerGroup, String nameServer,
			String topic, String subExpress) {
		super();
		this.consumerGroup = consumerGroup;
		this.nameServer = nameServer;
		this.topic = topic;
		this.subExpress = subExpress;
	}

	public int getMaxFailTimes() {
		return maxFailTimes;
	}

	public void setMaxFailTimes(int maxFailTimes) {
		this.maxFailTimes = maxFailTimes;
	}

	public Long getStartTimeStamp() {
		return startTimeStamp;
	}

	public void setStartTimeStamp(Long startTimeStamp) {
		this.startTimeStamp = startTimeStamp;
	}

	public Properties getPeroperties() {
		return peroperties;
	}

	public void setPeroperties(Properties peroperties) {
		this.peroperties = peroperties;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public String getNameServer() {
		return nameServer;
	}

	public String getTopic() {
		return topic;
	}

	public String getSubExpress() {
		return subExpress;
	}

	public int getBatchMsgNum() {
		return batchMsgNum;
	}

	public void setBatchMsgNum(int batchMsgNum) {
		this.batchMsgNum = batchMsgNum;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

}
