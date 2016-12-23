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

import org.apache.storm.pmml.model.ModelOutputs;
import org.apache.storm.pmml.runner.ModelRunnerFactory;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import javax.xml.bind.JAXBException;

/*
 * This class consists exclusively of static factory methods that create
 * object instances that are essential to work with the Jpmml library
 */
public class JpmmlFactory {
    /**
     * Creates a new {@link PMML} object representing the PMML model defined in the XML {@link File} specified as argument
     */
    public static PMML newPmml(File file) throws JAXBException, SAXException, IOException {
        Objects.requireNonNull(file);
        return IOUtil.unmarshal(file);
    }

    /**
     * Creates a new {@link PMML} object representing the PMML model defined in the {@link InputStream} specified as argument
     */
    public static PMML newPmml(InputStream stream) throws JAXBException, SAXException, IOException {
        Objects.requireNonNull(stream);
        return IOUtil.unmarshal(stream);
    }

    /**
     * Creates a new {@link Evaluator} object representing the PMML model defined in the {@link PMML} argument
     */
    public static Evaluator newEvaluator(PMML pmml) {
        Objects.requireNonNull(pmml);
        final PMMLManager pmmlManager = new PMMLManager(pmml);
        return (ModelEvaluator<?>) pmmlManager.getModelManager(null, ModelEvaluatorFactory.getInstance());
    }

    /**
     * Creates a new {@link Evaluator} object representing the PMML model defined in the XML {@link File} specified as argument
     */
    public static Evaluator newEvaluator(File file) throws IOException, JAXBException, SAXException {
        Objects.requireNonNull(file);
        return newEvaluator(newPmml(file));
    }

    /**
     * Creates a new {@link Evaluator} object representing the PMML model defined in the XML {@link File} specified as argument
     */
    public static Evaluator newEvaluator(InputStream stream) throws IOException, JAXBException, SAXException {
        Objects.requireNonNull(stream);
        return newEvaluator(newPmml(stream));
    }

    public static class ModelRunnerCreator implements ModelRunnerFactory {
        private File model;
        private ModelOutputs outFields;

        public ModelRunnerCreator(File model, ModelOutputs modelOutputs) {
            this.model = model;
            this.outFields = modelOutputs;
        }

        /**
         * Creates a {@link JPmmlModelRunner} writing to the default stream
         */
        @Override
        public org.apache.storm.pmml.runner.ModelRunner newModelRunner() {
            try {
                return new JPmmlModelRunner(JpmmlFactory.newEvaluator(model), outFields);
            } catch (Exception e) {
                throw new RuntimeException("Could not create ModelRunner from model " + model);
            }
        }
    }
}
