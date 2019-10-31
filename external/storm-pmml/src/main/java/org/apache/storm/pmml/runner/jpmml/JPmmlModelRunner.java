/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.pmml.runner.jpmml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.pmml.model.ModelOutputs;
import org.apache.storm.pmml.runner.PmmlModelRunner;
import org.apache.storm.tuple.Tuple;
import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.FieldValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JPMML implementation of {@link PmmlModelRunner}. It extracts the raw inputs from the tuple for all
 * 'active fields', and builds a tuple with the predicted scores for the 'predicted fields' and 'output fields'.
 * In this implementation all the declared streams will have the same scored tuple.
 *
 * <p>The 'predicted', 'active', and 'output' fields are extracted from the PMML model.
 */
public class JPmmlModelRunner implements PmmlModelRunner<Tuple,
        Map<FieldName, Object>,
        Map<FieldName, FieldValue>,
        Map<FieldName, ?>> {

    private static final Logger LOG = LoggerFactory.getLogger(JPmmlModelRunner.class);

    private final Evaluator eval;                       // Jpmml evaluator
    private final List<FieldName> activeFields;
    private final List<FieldName> predictedFields;
    private final List<FieldName> outputFields;
    private final ModelOutputs modelOutputs;

    public JPmmlModelRunner(Evaluator evaluator, ModelOutputs modelOutputs) {
        this.eval = evaluator;
        this.modelOutputs = modelOutputs;
        activeFields = evaluator.getActiveFields();
        predictedFields = eval.getPredictedFields();
        outputFields = eval.getOutputFields();
    }

    /**
     * Extract raw inputs.
     * @return The raw inputs extracted from the tuple for all 'active fields'
     */
    @Override
    public Map<FieldName, Object> extractRawInputs(Tuple tuple) {
        LOG.debug("Extracting raw inputs from tuple: = [{}]", tuple);
        final Map<FieldName, Object> rawInputs = new LinkedHashMap<>();
        for (FieldName activeField : activeFields) {
            rawInputs.put(activeField, tuple.getValueByField(activeField.getValue()));
        }
        LOG.debug("Raw inputs = [{}]", rawInputs);
        return rawInputs;
    }

    @Override
    public Map<FieldName, FieldValue> preProcessInputs(Map<FieldName, Object> rawInputs) {
        LOG.debug("Pre processing raw inputs: = [{}]", rawInputs);
        final Map<FieldName, FieldValue> preProcInputs = new LinkedHashMap<>();
        for (Map.Entry<FieldName, Object> rawEntry : rawInputs.entrySet()) {
            preProcInputs.putIfAbsent(rawEntry.getKey(), EvaluatorUtil.prepare(eval, rawEntry.getKey(), rawEntry.getValue()));
        }
        LOG.debug("Pre processed inputs = [{}]", preProcInputs);
        return preProcInputs;
    }

    @Override
    public Map<FieldName, ?> predictScores(Map<FieldName, FieldValue> preProcInputs) {
        LOG.debug("Predicting scores for pre processed inputs: = [{}]", preProcInputs);
        Map<FieldName, ?> predictedScores = eval.evaluate(preProcInputs);
        LOG.debug("Predicted scores = [{}]", predictedScores);
        return predictedScores;
    }

    /**
     * Retrieve scores.
     * @return the predicted scores for the 'predicted fields' and 'output fields'.
     *     All the declared streams will have the same scored tuple.
     */
    @Override
    public Map<String, List<Object>> scoredTuplePerStream(Tuple input) {
        final Map<FieldName, Object> rawInputs = extractRawInputs(input);
        final Map<FieldName, FieldValue> preProcInputs = preProcessInputs(rawInputs);
        final Map<FieldName, ?> predScores = predictScores(preProcInputs);

        return toValuesMap(predScores);
    }

    // Sends the same tuple (list of scored/predicted values) to all the declared streams
    private Map<String, List<Object>> toValuesMap(Map<FieldName, ?> predScores) {
        final List<Object> scoredVals = new ArrayList<>();

        for (FieldName predictedField : predictedFields) {
            Object targetValue = predScores.get(predictedField);
            scoredVals.add(EvaluatorUtil.decode(targetValue));
        }

        for (FieldName outputField : outputFields) {
            Object targetValue = predScores.get(outputField);
            scoredVals.add(EvaluatorUtil.decode(targetValue));
        }

        final Map<String, List<Object>> valuesMap = new HashMap<>();

        for (String stream: modelOutputs.streams()) {
            valuesMap.put(stream, scoredVals);
        }

        return valuesMap;
    }

    public Evaluator getEval() {
        return eval;
    }

    public List<FieldName> getActiveFields() {
        return Collections.unmodifiableList(activeFields);
    }

    public List<FieldName> getPredictedFields() {
        return Collections.unmodifiableList(predictedFields);
    }

    public List<FieldName> getOutputFields() {
        return Collections.unmodifiableList(outputFields);
    }

    public ModelOutputs getModelOutputs() {
        return modelOutputs;
    }
}
