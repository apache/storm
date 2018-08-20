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

package org.apache.storm.elasticsearch.response;

import java.util.List;

import org.apache.storm.elasticsearch.doc.IndexItemDoc;

/**
 * Mapped response for bulk index.
 */
public class BulkIndexResponse {

    private boolean errors;
    private long took;
    private List<IndexItemDoc> items;

    public BulkIndexResponse() {
    
    }

    public boolean hasErrors() {
        return errors;
    }

    public void setErrors(boolean errors) {
        this.errors = errors;
    }

    public long getTook() {
        return took;
    }

    public void setTook(long took) {
        this.took = took;
    }

    public List<IndexItemDoc> getItems() {
        return items;
    }

    public void setItems(List<IndexItemDoc> items) {
        this.items = items;
    }

    /**
     * Retrieve first error's code from response.
     *
     * @return error status code
     */
    public Integer getFirstError() {
        if (items == null || items.isEmpty()) {
            return null;
        }
        for (IndexItemDoc item : items) {
            int status = item.getIndex().getStatus();
            if (400 <= status && status <= 599) {
                return status;
            }
        }
        return null;
    }

    /**
     * Retrieve first result from response.
     *
     * @return result text
     */
    public String getFirstResult() {
        if (items == null || items.isEmpty()) {
            return null;
        }
        return items.get(0).getIndex().getResult();
    }
}
