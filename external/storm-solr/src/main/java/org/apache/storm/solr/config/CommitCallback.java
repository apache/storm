/*
 * OnCommit.java
 * Copyright 2017 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package org.apache.storm.solr.config;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.io.Serializable;

/**
 * This callback interface is invoked on {@link SolrCommitStrategy} triggers or tick. Typically fire
 * a "commit" request to solr, you can customize commit details by implementing this interface.
 * @author alei
 */
public interface CommitCallback extends Serializable {

    void process(SolrClient solrClient, String collection) throws SolrServerException, IOException;
}
