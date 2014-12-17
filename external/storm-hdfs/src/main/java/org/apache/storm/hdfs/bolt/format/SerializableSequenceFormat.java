/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hdfs.bolt.format;

import backtype.storm.tuple.Tuple;

/**
 * Interface for converting <code>Tuple</code> objects to HDFS sequence file key-value pairs.
 * This is an example using Avro:
 * <code>
 *	public class AvroSequenceFormat implements SerializableSequenceFormat {
 *	  private String keyField;
 *	  private String valueField;
 *
 *	  public AvroSequenceFile(String keyField, String valueField) {
 *		this.keyField = keyField;
 *		this.valueField = valueField;
 *	  }
 *
 *	  public Class keyClass() {
 *		return AvroKey.class;
 *	  }
 *
 *	  public Class valueClass() {
 *	   	return AvroValue.class;
 *	  }
 *
 *	  public Writable key(Tuple tuple) {
 *	 	return null;
 *	  }
 *
 *    public Writable value(Tuple tuple) {
 *    	return null;
 *    }
 *
 *    public Object keyObject(Tuple tuple) {
 *    	String avroKeyJson = tuple.getStringByField(keyField);
 *    	GenericRecord id = //Parse into avro type
 *
 *    	return id;
 *    }
 *
 *    public Object valueObject(Tuple tuple) {
 *    	String avroValueJson = tuple.getStringByField(valueField);
 *    	GenericRecord value = //Parse into avro type
 *
 *    	return value;
 *    }
 * }
 * </code>
 *
 */
public interface SerializableSequenceFormat extends SequenceFormat {

	/**
	* Process and return a tuple as a key object for the sequence file.
	* @return Object
	*/
    Object keyObject(Tuple tuple);

    /**
	* Process and return a tuple as a value object for the sequence file.
	* @return Object
	*/
    Object valueObject(Tuple tuple);
}