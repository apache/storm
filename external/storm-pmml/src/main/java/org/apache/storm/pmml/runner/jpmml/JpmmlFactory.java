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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

import javax.xml.bind.JAXBException;

import org.apache.storm.pmml.model.ModelOutputs;
import org.apache.storm.pmml.runner.ModelRunner;
import org.apache.storm.pmml.runner.ModelRunnerFactory;
import org.apache.storm.utils.Utils;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.xml.sax.SAXException;

/*
 * This class consists exclusively of static factory methods that create instances that are essential to work with the
 *  Jpmml library.
 */
public class JpmmlFactory {

    /**
     * Creates a new {@link PMML} object representing the PMML model defined in the XML {@link File} specified as argument.
     */
    public static PMML newPmml(File file) throws JAXBException, SAXException, IOException {
        Objects.requireNonNull(file);
        return IOUtil.unmarshal(file);
    }

    /**
     * Creates a new {@link PMML} object representing the PMML model defined in the {@link InputStream} specified as argument.
     */
    public static PMML newPmml(InputStream stream) throws JAXBException, SAXException, IOException {
        Objects.requireNonNull(stream);
        return IOUtil.unmarshal(stream);
    }

    /**
     * Creates a new {@link PMML} object representing the PMML model uploaded to the Blobstore with key specified as
     * argument. Uses Storm config as returned by {@code Utils.readStormConfig()} to get the Blobstore client.
     */
    public static PMML newPmml(String blobKey) throws JAXBException, SAXException, IOException {
        return newPmml(blobKey, Utils.readStormConfig());
    }

    /**
     * Creates a new {@link PMML} object representing the PMML model uploaded to the Blobstore with key specified as
     * argument. Uses the specified configuration to get the Blobstore client.
     */
    public static PMML newPmml(String blobKey, Map<String, Object> config) throws JAXBException, SAXException, IOException {
        Objects.requireNonNull(blobKey);
        Objects.requireNonNull(config);
        return newPmml(getPmmlModelBlob(blobKey, config));
    }

    // ==================   Get PMML Model from Blobstore ==================

    /**
     * Returns PMML model from Blobstore. Uses Storm config as returned by {@code Utils.readStormConfig()}.
     * @param blobKey key of PMML model in Blobstore
     */
    public static InputStream getPmmlModelBlob(String blobKey) {
        return getPmmlModelBlob(blobKey, Utils.readStormConfig());
    }

    /**
     * Returns PMML model from Blobstore.
     * @param blobKey key of PMML model in Blobstore
     * @param config Configuration to use to get Blobstore client
     */
    public static InputStream getPmmlModelBlob(String blobKey, Map<String, Object> config) {
        Objects.requireNonNull(blobKey);
        Objects.requireNonNull(config);
        try {
            return Utils.getClientBlobStore(config).getBlob(blobKey);
        } catch (Exception e) {
            throw new RuntimeException("Failed to download PMML Model from Blobstore using blob key ["
                    + blobKey
                    + "]",
                    e);
        }
    }

    // ==================   Evaluator   ==================

    /**
     * Creates a new {@link Evaluator} object representing the PMML model defined in the {@link PMML} argument.
     */
    public static Evaluator newEvaluator(PMML pmml) {
        Objects.requireNonNull(pmml);
        final PMMLManager pmmlManager = new PMMLManager(pmml);
        return (ModelEvaluator<?>) pmmlManager.getModelManager(null, ModelEvaluatorFactory.getInstance());
    }

    /**
     * Creates a new {@link Evaluator} object representing the PMML model defined in the XML {@link File} specified as
     * argument.
     */
    public static Evaluator newEvaluator(File file) throws IOException, JAXBException, SAXException {
        Objects.requireNonNull(file);
        return newEvaluator(newPmml(file));
    }

    /**
     * Creates a new {@link Evaluator} object representing the PMML model defined in the XML {@link File} specified as
     * argument.
     */
    public static Evaluator newEvaluator(InputStream stream) throws IOException, JAXBException, SAXException {
        Objects.requireNonNull(stream);
        return newEvaluator(newPmml(stream));
    }

    /**
     * Creates a new {@link Evaluator} object representing the PMML model uploaded to the Blobstore using the blob key
     * specified as argument. Uses Storm config as returned by {@code Utils.readStormConfig()} to get the Blobstore
     * client.
     */
    public static Evaluator newEvaluator(String blobKey) throws IOException, JAXBException, SAXException {
        Objects.requireNonNull(blobKey);
        return newEvaluator(blobKey, Utils.readStormConfig());
    }

    /**
     * Creates a new {@link Evaluator} object representing the PMML model uploaded to the Blobstore using the blob key
     * specified as argument. Uses the specified configuration to get the Blobstore client.
     */
    public static Evaluator newEvaluator(String blobKey, Map<String, Object> config) throws IOException, JAXBException, SAXException {
        Objects.requireNonNull(blobKey);
        Objects.requireNonNull(config);
        return newEvaluator(newPmml(blobKey, config));
    }

    // ============  Factories  ============

    public static class ModelRunnerFromFile implements ModelRunnerFactory {
        private final File model;
        private final ModelOutputs outFields;

        public ModelRunnerFromFile(File model, ModelOutputs modelOutputs) {
            Objects.requireNonNull(model);
            Objects.requireNonNull(modelOutputs);

            this.model = model;
            this.outFields = modelOutputs;
        }

        /**
         * Creates a {@link JPmmlModelRunner} writing to the default stream.
         */
        @Override
        public ModelRunner newModelRunner() {
            try {
                return new JPmmlModelRunner(JpmmlFactory.newEvaluator(model), outFields);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create ModelRunner from model " + model, e);
            }
        }
    }

    /**
     * Creates a {@link JPmmlModelRunner} writing to the default stream. PMML Model is downloaded from Blobstore.
     */
    public static class ModelRunnerFromBlobStore implements ModelRunnerFactory {
        private final String blobKey;
        private final ModelOutputs modelOutputs;
        private final Map<String, Object> config;

        /**
         * Uses Storm config as returned by {@code Utils.readStormConfig()}.
         * @param blobKey key of PMML model in Blobstore
         */
        public ModelRunnerFromBlobStore(String blobKey, ModelOutputs modelOutputs) {
            this(blobKey, modelOutputs, Utils.readStormConfig());
        }

        /**
         * Create a new {@code ModelRunnerFromBlobStore}.
         * @param blobKey key of PMML model in Blobstore
         * @param config Configuration to use to get Blobstore client
         */
        public ModelRunnerFromBlobStore(String blobKey, ModelOutputs modelOutputs, Map<String, Object> config) {
            Objects.requireNonNull(blobKey);
            Objects.requireNonNull(modelOutputs);
            Objects.requireNonNull(config);

            this.blobKey = blobKey;
            this.modelOutputs = modelOutputs;
            this.config = config;
        }

        @Override
        public ModelRunner newModelRunner() {
            try {
                return new JPmmlModelRunner(JpmmlFactory.newEvaluator(blobKey, config), modelOutputs);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to create ModelRunner from model in Blobstore "
                        + "using blob key [%s] and config [%s]",
                        blobKey,
                        config),
                        e);
            }
        }
    }
}
