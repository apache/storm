#!/bin/bash

jstorm jar target/sequence-split-merge-1.0.8-jar-with-dependencies.jar com.alipay.dw.jstorm.example.sequence.SequenceTopology conf/conf.prop
#jstorm jar target/sequence-split-merge-1.0.8-jar-with-dependencies.jar com.alipay.dw.jstorm.example.batch.SimpleBatchTopology conf/topology.yaml
