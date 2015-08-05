#!/bin/bash

#jstorm jar target/sequence-split-merge-1.1.0-jar-with-dependencies.jar com.alipay.dw.jstorm.transcation.TransactionalGlobalCount global
jstorm jar target/sequence-split-merge-1.1.0-jar-with-dependencies.jar com.alipay.dw.jstorm.example.sequence.SequenceTopology conf/conf.yaml
#jstorm jar target/sequence-split-merge-1.0.8-jar-with-dependencies.jar com.alipay.dw.jstorm.example.sequence.SequenceTopology conf/conf.prop
#jstorm jar target/sequence-split-merge-1.0.8-jar-with-dependencies.jar com.alipay.dw.jstorm.example.batch.SimpleBatchTopology conf/topology.yaml
