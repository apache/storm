package com.alibaba.jstorm.task.group;

/**
 * Grouping type
 * 
 * @author yannian
 * 
 */
public enum GrouperType {
	global, fields, all, shuffle, none, custom_obj, custom_serialized, direct, local_or_shuffle, localFirst
}
