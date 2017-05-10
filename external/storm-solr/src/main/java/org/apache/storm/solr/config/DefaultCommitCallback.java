/*
 * DefaultCommitOperation.java
 * Copyright 2017 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package org.apache.storm.solr.config;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;

/**
 * @author alei
 */
public class DefaultCommitCallback implements CommitCallback {

    @Override
    public void process(final SolrClient solrClient, final String collection) throws SolrServerException, IOException {
        solrClient.commit(collection);
    }
}
