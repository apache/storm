/*
 * SolrClientSchemaBuilder.java
 * Copyright 2017 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package org.apache.storm.solr.schema.builder;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.storm.solr.client.SolrClientFactory;
import org.apache.storm.solr.schema.CopyField;
import org.apache.storm.solr.schema.FieldType;
import org.apache.storm.solr.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>this Class build the {@link Schema} by Solr Schema API.</p>
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/solr/Schema+API">Solr Schema API</a>
 * @author alei
 */
public class SolrClientSchemaBuilder implements SchemaBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(SolrClientSchemaBuilder.class);

    private Schema schema;

    public SolrClientSchemaBuilder(String collection, SolrClientFactory factory) throws IOException, SolrServerException {
        SchemaRequest request = new SchemaRequest();
        SchemaResponse response = request.process(factory.getSolrClient(), collection);
        SchemaRepresentation representation = response.getSchemaRepresentation();
        schema = convert(representation);
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    private Schema convert(final SchemaRepresentation representation) {
        Schema converted = new Schema();
        converted.setUniqueKey(representation.getUniqueKey());
        converted.setName(representation.getName());
        converted.setVersion(String.valueOf(representation.getVersion()));
        List<FieldType> fieldTypeList = new ArrayList<>(representation.getFieldTypes().size());
        for (FieldTypeDefinition type : representation.getFieldTypes()) {
            FieldType fieldType = new FieldType();
            fieldType.setClazz(type.getAttributes().get("class").toString());
            if (type.getAttributes().get("multiValued") != null) {
                fieldType.setMultiValued(Boolean.valueOf(type.getAttributes().get("multiValued").toString()));
            }
            fieldType.setName(type.getAttributes().get("name").toString());
            fieldTypeList.add(fieldType);
        }
        converted.setFieldTypes(fieldTypeList);
        List<org.apache.storm.solr.schema.Field> fieldList =
                new ArrayList<>(representation.getFields().size());
        for (Map<String, Object> field : representation.getFields()) {
            org.apache.storm.solr.schema.Field schemaField = new org.apache.storm.solr.schema.Field();
            schemaField.setName(field.get("name").toString());
            schemaField.setType(field.get("type").toString());
            fieldList.add(schemaField);
        }
        converted.setFields(fieldList);
        List<org.apache.storm.solr.schema.Field> dynamicFieldList =
                new ArrayList<>(representation.getDynamicFields().size());
        for (Map<String, Object> field : representation.getDynamicFields()) {
            org.apache.storm.solr.schema.Field schemaField = new org.apache.storm.solr.schema.Field();
            schemaField.setName(field.get("name").toString());
            schemaField.setType(field.get("type").toString());
            dynamicFieldList.add(schemaField);
        }
        converted.setDynamicFields(dynamicFieldList);
        List<CopyField> copyFieldList = new ArrayList<>(representation.getCopyFields().size());
        for (Map<String, Object> field : representation.getDynamicFields()) {
            CopyField copyField = new CopyField();
            copyField.setDest(field.get("dest").toString());
            copyField.setSource(field.get("source").toString());
            copyFieldList.add(copyField);
        }
        converted.setCopyFields(copyFieldList);
        return converted;
    }
    
}
