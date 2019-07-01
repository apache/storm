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

package org.apache.storm.pmml.runner;

import org.apache.storm.tuple.Tuple;

/**
 * Runner for models defined using PMML.
 *
 * @param <I> type of the input source.  For Storm it is typically a {@link Tuple}
 * @param <R> the type of extracted raw input
 * @param <P> the type of preprocessed input
 * @param <S> the type of predicted scores
 */
public interface PmmlModelRunner<I, R, P, S> extends ModelRunner {

    /**
     * Extracts from the tuple the raw inputs that are to be scored according to the predictive model.
     *
     * @param input source from which to extract raw inputs
     * @return raw inputs
     */
    R extractRawInputs(I input);

    /**
     * Pre process inputs, i.e., remove missing fields, outliers, etc
     *
     * @param rawInputs that are to be preprocessed
     * @return preprocessed output
     */
    P preProcessInputs(R rawInputs);

    /**
     * Compute the predicted scores from the pre-processed inputs in the step above.
     *
     * @param preProcInputs that are to be preprocessed
     * @return predicted scores
     */
    S predictScores(P preProcInputs);
}
