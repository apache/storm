/*******************************************************************************
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
 *******************************************************************************/
package org.apache.storm.eventhubs.state;

import java.io.Serializable;

/**
 * Contracts for persisting checkpoint data
 *
 */
public interface IStateStore extends Serializable {

	/**
	 * Open/initialize connection to the persistent store.
	 */
	public void open();

	/**
	 * Cleanup and sever the connection to persistent store.
	 */
	public void close();

	/**
	 * Persist the given data against the specified path.
	 * 
	 * @param path
	 *            key/path to save the data at.
	 * @param data
	 *            data to be saved
	 */
	public void saveData(String path, String data);

	/**
	 * Retrieve information stored against the specified key/path
	 * 
	 * @param path
	 *            key/path to get data from
	 * @return
	 */
	public String readData(String path);
}
