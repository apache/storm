package com.dianping.cosmos.util;

import org.apache.commons.lang.StringUtils;

public class CatMetricUtil {
    private static final String CAT_METRIC_NAME_PREFIX = "Cat#"; 
    
    /**
     * 返回BlackHoleSout的metric名称
     * @param topic
     * @param group
     * @return
     */
    public static String getSpoutMetricName(String topic, String group){
        return CAT_METRIC_NAME_PREFIX.concat(topic).concat("[").concat(group).concat("]");
    }
    
    /**
     * 判断是否cat的metirc
     * @param dataPointName
     * @return
     */
    public static boolean isCatMetric(String dataPointName){
        if(StringUtils.isBlank(dataPointName)){
            return false;
        }
        return StringUtils.startsWith(dataPointName, CAT_METRIC_NAME_PREFIX);
    }
    
    
    
    /**
     * 根据metric的名字，返回写入cat上的key
     * @param spoutMetricName
     * @return
     */
    public static String getCatMetricKey(String spoutMetricName){
        if(StringUtils.isBlank(spoutMetricName) 
                || !StringUtils.startsWith(spoutMetricName, CAT_METRIC_NAME_PREFIX)){
            return "default";
        }
        return StringUtils.substringAfter(spoutMetricName, CAT_METRIC_NAME_PREFIX);
        
    }
}
