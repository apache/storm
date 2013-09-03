package com.alipay.dw.jstorm.task.group;

import java.util.List;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

/**
 * user defined grouping method
 * 
 * @author Longda/yannian
 * 
 */
public class MkCustomGrouper {
    private CustomStreamGrouping grouping;
    
    public MkCustomGrouper(TopologyContext context, 
            CustomStreamGrouping _grouping, Fields _out_fields,
            List<Integer> targetTask) {
        this.grouping = _grouping;
        this.grouping.prepare(context, _out_fields, targetTask);
    }
    
    public List<Integer> grouper(List<Object> values) {
        return this.grouping.chooseTasks(values);
    }
}
