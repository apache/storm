---
title: Storm PMML Bolt
layout: documentation
documentation: true
---

 Storm integration to load PMML models and compute predictive scores for running tuples. The PMML model represents
 the machine learning (predictive) model used to do prediction on raw input data. The model is typically loaded into a 
 runtime environment, which will score the raw data that comes in the tuples. 

## Create Instance of PMML Bolt
 To create an instance of the `PMMLPredictorBolt`, you must provide the `ModelOutputs`, and a `ModelRunner` using a 
 `ModelRunnerFactory`. The `ModelOutputs` represents the streams and output fields declared by the `PMMLPredictorBolt`.
 The `ModelRunner` represents the runtime environment to execute the predictive scoring. It has only one method: 
 
 ```java
    Map<String, List<Object>> scoredTuplePerStream(Tuple input); 
 ```
 
 This method contains the logic to compute the scored tuples from the raw inputs tuple.  It's up to the discretion of the 
 implementation to define which scored values are to be assigned to each stream. The keys of this map are the stream ids, 
 and the values the predicted scores. 
   
 The `PmmlModelRunner` is an extension of `ModelRunner` that represents the typical steps involved 
 in predictive scoring. Hence, it allows for the **extraction** of raw inputs from the tuple, **pre process** the 
 raw inputs, and **predict** the scores from the preprocessed data.
 
 The `JPmmlModelRunner` is an implementation of `PmmlModelRunner` that uses [JPMML](https://github.com/jpmml/jpmml) as
 runtime environment. This implementation extracts the raw inputs from the tuple for all `active fields`, 
 and builds a tuple with the predicted scores for the `predicted fields` and `output fields`. 
 In this implementation all the declared streams will have the same scored tuple.
 
 The `predicted`, `active`, and `output` fields are extracted from the PMML model.

## Run Bundled Examples

To run the examples you must execute the following command:
 
 ```java
 STORM-HOME/bin/storm jar STORM-HOME/examples/storm-pmml-examples/storm-pmml-examples-2.0.0-SNAPSHOT.jar 
 org.apache.storm.pmml.JpmmlRunnerTestTopology jpmmlTopology PMMLModel.xml RawInputData.csv
 ```
