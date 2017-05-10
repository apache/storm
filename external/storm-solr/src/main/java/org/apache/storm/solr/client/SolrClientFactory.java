/*
 * SerializableSolrClientFactory.java
 * Copyright 2017 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package org.apache.storm.solr.client;

import org.apache.solr.client.solrj.SolrClient;

import java.io.Serializable;

/**
 * this interface provide SolrClient for {@link org.apache.storm.solr.mapper.SolrMapper}
 *
 * @author alei
 */
public interface SolrClientFactory extends Serializable {

    SolrClient getSolrClient();
}
