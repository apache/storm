package com.alibaba.jstorm.ons;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.aliyun.openservices.ons.api.PropertyKeyConst;

public class OnsConfig implements Serializable{

	private static final long serialVersionUID = -3911741873533333336L;
	
	private final String topic;
	private final String subExpress;
	private final String accessKey;
	private final String secretKey;
	
	public OnsConfig(Map conf) {
		topic = (String)conf.get("Topic");
		if (conf.get("SubExpress") != null) {
			subExpress = (String)conf.get("SubExpress");
		}else {
			subExpress = "*";
		}
		accessKey = (String)conf.get(PropertyKeyConst.AccessKey);
		secretKey = (String)conf.get(PropertyKeyConst.SecretKey);
		
		checkValid();
		
	}
	
	public void checkValid() {
		if (StringUtils.isBlank(topic) == true) {
			throw new RuntimeException("Topic hasn't been set");
		}else if (StringUtils.isBlank(subExpress)) {
			throw new RuntimeException("SubExpress hasn't been set");
		}else if (StringUtils.isBlank(accessKey)) {
			throw new RuntimeException(PropertyKeyConst.AccessKey + " hasn't been set");			
		}else if (StringUtils.isBlank(secretKey)) {
			throw new RuntimeException(PropertyKeyConst.SecretKey + " hasn't been set");
		}
		
	}

	public String getTopic() {
		return topic;
	}

	public String getSubExpress() {
		return subExpress;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}
	
	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
