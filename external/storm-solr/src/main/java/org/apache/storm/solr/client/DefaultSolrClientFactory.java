/*
 * DefaultCloudSolrClientFactory.java
 * Copyright 2017 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package org.apache.storm.solr.client;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

/**
 * default implementation of {@link SolrClientFactory}, which takes a zookeeper host
 * and produce {@link org.apache.solr.client.solrj.impl.CloudSolrClient} with the host.
 *
 * @author alei
 */
public class DefaultSolrClientFactory implements SolrClientFactory {

    private String zkHostString;

    public DefaultSolrClientFactory(String zkHostString) {
        this.zkHostString = zkHostString;
    }

    @Override
    public SolrClient getSolrClient() {
        return new CloudSolrClient(zkHostString);
    }
}
