/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.solr.schema.builder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.storm.solr.config.SolrConfig;
import org.apache.storm.solr.schema.CopyField;
import org.apache.storm.solr.schema.Field;
import org.apache.storm.solr.schema.FieldType;
import org.apache.storm.solr.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that builds the {@link Schema} object from the schema returned by the SchemaRequest.
 */
public class RestJsonSchemaBuilderV2 implements SchemaBuilder {
    private static final Logger logger = LoggerFactory.getLogger(RestJsonSchemaBuilderV2.class);
    private Schema schema = new Schema();
    private SolrConfig solrConfig;
    private String collection;

    public RestJsonSchemaBuilderV2(SolrConfig solrConfig, String collection) {
        this.solrConfig = solrConfig;
        this.collection = collection;
    }

    @Override
    public void buildSchema() throws IOException {
        SolrClient solrClient = null;
        try {
            solrClient = new CloudSolrClient(solrConfig.getZkHostString());
            SchemaRequest schemaRequest = new SchemaRequest();
            logger.debug("Downloading schema for collection: {}", collection);
            SchemaResponse schemaResponse = schemaRequest.process(solrClient, collection);
            logger.debug("SchemaResponse Schema: {}", schemaResponse);
            SchemaRepresentation schemaRepresentation = schemaResponse.getSchemaRepresentation();

            schema.setName(schemaRepresentation.getName());
            schema.setVersion(Float.toString(schemaRepresentation.getVersion()));
            schema.setUniqueKey(schemaRepresentation.getUniqueKey());
            schema.setFieldTypes(getFieldTypes(schemaRepresentation));
            schema.setFields(getFields(schemaRepresentation.getFields()));
            schema.setDynamicFields(getFields(schemaRepresentation.getDynamicFields()));
            schema.setCopyFields(getCopyFields(schemaRepresentation));
        } catch (SolrServerException e) {
            logger.error("Error while getting schema for collection: {}", collection, e);
            throw new IOException("Error while getting schema for collection :" + collection, e);
        } finally {
            if (solrClient != null) {
                solrClient.close();
            }
        }
    }

    private List<FieldType> getFieldTypes(SchemaRepresentation schemaRepresentation) {
        List<FieldType> fieldTypes = new LinkedList<>();
        for (FieldTypeDefinition fd : schemaRepresentation.getFieldTypes()) {
            FieldType ft = new FieldType();
            ft.setName((String) fd.getAttributes().get("name"));
            ft.setClazz((String) fd.getAttributes().get("class"));
            Object multiValued = fd.getAttributes().get("multiValued");
            if (multiValued != null) {
                ft.setMultiValued((Boolean) multiValued);
            }
            fieldTypes.add(ft);
        }
        return fieldTypes;
    }

    private List<Field> getFields(List<Map<String, Object>> schemaFields) {
        List<Field> fields = new LinkedList<>();
        for (Map<String, Object> map : schemaFields) {
            Field field = new Field();
            field.setName((String) map.get("name"));
            field.setType((String) map.get("type"));
            fields.add(field);
        }
        return fields;
    }

    private List<CopyField> getCopyFields(SchemaRepresentation schemaRepresentation) {
        List<CopyField> copyFields = new LinkedList<>();
        for (Map<String, Object> map : schemaRepresentation.getCopyFields()) {
            CopyField cp = new CopyField();
            cp.setSource((String) map.get("source"));
            cp.setDest((String) map.get("dest"));
            copyFields.add(cp);
        }
        return copyFields;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
