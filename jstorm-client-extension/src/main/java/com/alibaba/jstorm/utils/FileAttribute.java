package com.alibaba.jstorm.utils;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.json.simple.JSONAware;

//import com.alibaba.fastjson.JSONAware;

public class FileAttribute implements Serializable, JSONAware {

	/**  */
	private static final long serialVersionUID = -5131640995402822835L;

	private String fileName;
	private String isDir;
	private String modifyTime;
	private String size;

	private static final String FILE_NAME_FIELD = "fileName";
	private static final String IS_DIR_FIELD = "isDir";
	private static final String MODIFY_TIME_FIELD = "modifyTime";
	private static final String SIZE_FIELD = "size";

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getIsDir() {
		return isDir;
	}

	public void setIsDir(String isDir) {
		this.isDir = isDir;
	}

	public String getModifyTime() {
		return modifyTime;
	}

	public void setModifyTime(String modifyTime) {
		this.modifyTime = modifyTime;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

	@Override
	public String toJSONString() {
		Map<String, String> map = new HashMap<String, String>();

		map.put(FILE_NAME_FIELD, fileName);
		map.put(IS_DIR_FIELD, isDir);
		map.put(MODIFY_TIME_FIELD, modifyTime);
		map.put(SIZE_FIELD, size);
		return JStormUtils.to_json(map);
	}

	public static FileAttribute fromJSONObject(Map jobj) {
		if (jobj == null) {
			return null;
		}

		FileAttribute attribute = new FileAttribute();

		attribute.setFileName((String) jobj.get(FILE_NAME_FIELD));
		attribute.setIsDir((String) jobj.get(IS_DIR_FIELD));
		attribute.setModifyTime((String) jobj.get(MODIFY_TIME_FIELD));
		attribute.setSize((String) jobj.get(SIZE_FIELD));

		return attribute;
	}

	public static void main(String[] args) {
		Map<String, FileAttribute> map = new HashMap<String, FileAttribute>();

		FileAttribute attribute = new FileAttribute();
		attribute.setFileName("test");
		attribute.setIsDir("true");
		attribute.setModifyTime(new Date().toString());
		attribute.setSize("4096");

		map.put("test", attribute);
		
		System.out.println("Before:" + map);

		String jsonString = JStormUtils.to_json(map);

		Map<String, Map> map2 = (Map<String, Map>) JStormUtils
				.from_json(jsonString);

		Map jObject = map2.get("test");

		FileAttribute attribute2 = FileAttribute.fromJSONObject(jObject);
		
		System.out.println("attribute2:" + attribute2);
	}

}
