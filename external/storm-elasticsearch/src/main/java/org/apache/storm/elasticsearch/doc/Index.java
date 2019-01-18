/**
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

package org.apache.storm.elasticsearch.doc;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Elasticsearch document fragment with "_index", "_type" and "_id" fields.
 */
public class Index {

    public Index() {

    }

    /**
     * Create a Index with the specified index, type and id.
     *
     * @param index index name
     * @param type  document type to be stored
     * @param id    unique document id in Elasticsearch
     */
    public Index(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    @JsonProperty("_index")
    private String index;

    @JsonProperty("_type")
    private String type;

    @JsonProperty("_id")
    private String id;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
