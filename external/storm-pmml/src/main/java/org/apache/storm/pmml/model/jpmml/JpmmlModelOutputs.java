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

package org.apache.storm.pmml.model.jpmml;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.storm.pmml.model.ModelOutputs;
import org.apache.storm.pmml.runner.jpmml.JpmmlFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;

public class JpmmlModelOutputs implements ModelOutputs {
    private final Map<String, ? extends Fields> declared;

    public JpmmlModelOutputs(Map<String, ? extends Fields> declaredFields) {
        this.declared = declaredFields;
    }

    @Override
    public Map<String, ? extends Fields> streamFields() {
        return declared;
    }

    @Override
    public String toString() {
        return "JpmmlModelOutputs{" + declared + '}';
    }

    // =================  Factory Methods Declaring ModelOutputs to Default Stream  ==================

    /**
     * Factory method that creates an instance of {@link ModelOutputs} that declares
     * the {@code predicted} and {@code output} fields specified in the {@link PMML} model
     * specified as argument into the {@code default} stream.
     */
    public static ModelOutputs toDefaultStream(PMML pmmlModel) {
        Objects.requireNonNull(pmmlModel);
        return create(pmmlModel, Collections.singletonList(Utils.DEFAULT_STREAM_ID));
    }

    public static ModelOutputs toDefaultStream(File pmmlModel) {
        try {
            return toDefaultStream(JpmmlFactory.newPmml(pmmlModel));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ModelOutputs toDefaultStream(InputStream pmmlModel) {
        try {
            return toDefaultStream(JpmmlFactory.newPmml(pmmlModel));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ModelOutputs toDefaultStream(String blobKey) {
        return toDefaultStream(blobKey, Utils.readStormConfig());
    }

    public static ModelOutputs toDefaultStream(String blobKey, Map<String, Object> config) {
        try {
            return toDefaultStream(JpmmlFactory.newPmml(blobKey, config));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // =================  Factory Methods Declaring ModelOutputs to Multiple Streams  ==================

    /**
     * Factory method that creates an instance of {@link ModelOutputs} that declares
     * the {@code predicted} and {@code output} fields specified in the {@link PMML} model
     * specified as argument into the list of streams specified.
     */
    public static ModelOutputs toStreams(PMML pmmlModel, List<String> streams) {
        return create(pmmlModel, streams);
    }

    public static ModelOutputs toStreams(File pmmlModel, List<String> streams) {
        try {
            return toStreams(JpmmlFactory.newPmml(pmmlModel), streams);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ModelOutputs toStreams(InputStream pmmlModel, List<String> streams) {
        try {
            return toStreams(JpmmlFactory.newPmml(pmmlModel), streams);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ModelOutputs toStreams(String blobKey, List<String> streams) {
        return toStreams(blobKey, Utils.readStormConfig(), streams);
    }

    public static ModelOutputs toStreams(String blobKey, Map<String, Object> config, List<String> streams) {
        try {
            return toStreams(JpmmlFactory.newPmml(blobKey, config), streams);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ======

    private static ModelOutputs create(PMML pmmlModel, List<String> streams) {
        final Set<String> fieldNames = new LinkedHashSet<>();
        final Evaluator evaluator = JpmmlFactory.newEvaluator(pmmlModel);

        for (FieldName predictedField : evaluator.getPredictedFields()) {
            fieldNames.add(predictedField.getValue());
        }

        for (FieldName outputField : evaluator.getOutputFields()) {
            fieldNames.add(outputField.getValue());
        }

        final Map<String, Fields> toDeclare = streams.stream()
                .collect(Collectors.toMap(Function.identity(), (x) -> new Fields(new ArrayList<>(fieldNames))));

        return new JpmmlModelOutputs(toDeclare);
    }
}
