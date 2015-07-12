package com.dianping.cosmos.util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSON工具类
 * @author xinchun.wang
 *
 */
public class JSONUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(JSONUtil.class);
	
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
	private static final JSONUtil INSTANCE = new JSONUtil();

	public static JSONUtil getInstance() {
		return INSTANCE;
	}

	private JSONUtil() {

	}

	/**
	 * 将map转化为json
	 * 
	 * @param map
	 * @return
	 */
	public String formatMap2JSON(Map<String, Object> map) {
		StringWriter stringWriter = new StringWriter();
		String json = "";
		try {
			JsonGenerator gen = new JsonFactory()
					.createJsonGenerator(stringWriter);
			MAPPER.writeValue(gen, map);
			gen.close();
			json = stringWriter.toString();
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return json;
	}
	
	/**
	 * POJO对象转换为JSON
	 * @param pojo
	 * @return
	 */
	public String formatPOJO2JSON(Object pojo) {
		StringWriter stringWriter = new StringWriter();
		String json = "";
		try {
			JsonGenerator gen = new JsonFactory()
					.createJsonGenerator(stringWriter);
			MAPPER.writeValue(gen, pojo);
			gen.close();
			json = stringWriter.toString();
		} catch (Exception e) {
			LOGGER.error(pojo.getClass().getName() + "转json出错", e);
		}
		return json;
	}
	
	/**
	 * 将json转化为map
	 * 
	 * @param json
	 * @return
	 */
	public Map<?, ?> formatJSON2Map(String json) {
		Map<?, ?> map = null;
		try {
			map = MAPPER.readValue(json, Map.class);
		} catch (Exception e) {
			LOGGER.error("formatJsonToMap error, json = " + json, e);
		}
		return map;
	}
	
	
	
	/**
	 * JSON转换为List
	 * @param json
	 * @return
	 */
	public List<?> formatJSON2List(String json) {
		List<?> list = null;
		try {
			list = MAPPER.readValue(json, List.class);
		} catch (Exception e) {
			LOGGER.error("formatJSON2List error, json = " + json, e);
		}
		return list;
	}
	
	public boolean equals(String firstJSON, String secondJSON) {
		try {
			JsonNode tree1 = MAPPER.readTree(firstJSON);
			JsonNode tree2 = MAPPER.readTree(secondJSON);
			boolean areEqual = tree1.equals(tree2);
			return areEqual;
		} catch (JsonProcessingException e) {
			LOGGER.error("json compare wrong:" + firstJSON + ";" + secondJSON,
					e);
		} catch (IOException e) {
			LOGGER.error("json compare wrong:" + firstJSON + ";" + secondJSON,
					e);
		}
		return false;
	}
}
