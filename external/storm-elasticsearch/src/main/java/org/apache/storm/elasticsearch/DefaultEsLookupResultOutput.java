/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.storm.elasticsearch.response.LookupResponse;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.elasticsearch.client.Response;

/**
 * Default implementation of {@link EsLookupResultOutput}.
 * Outputs the index, type, id and source as strings.
 */
public class DefaultEsLookupResultOutput implements EsLookupResultOutput {
    
    private static final long serialVersionUID = 2932278450655703239L;
    
    private ObjectMapper objectMapper;

    public DefaultEsLookupResultOutput(ObjectMapper objectMapper) {
        super();
        this.objectMapper = objectMapper;
    }

    @Override
    public Collection<Values> toValues(Response response) {
        LookupResponse lookupResponse;
        try {
            lookupResponse = objectMapper.readValue(response.getEntity().getContent(), LookupResponse.class);
        } catch (UnsupportedOperationException | IOException e) {
            throw new IllegalArgumentException("Response " + response + " is invalid", e);
        }
        return Collections.singleton(new Values(
                lookupResponse.getIndex(),
                lookupResponse.getType(),
                lookupResponse.getId(),
                lookupResponse.getSource()));
    }

    @Override
    public Fields fields() {
        return new Fields("index", "type", "id", "source");
    }

}
