/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.beam.translation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.storm.beam.translation.runtime.DoFnBolt;
import org.apache.storm.beam.translation.util.DefaultSideInputReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates a ParDo.Bound to a Storm DoFnBolt
 */
public class ParDoBoundTranslator<InputT, OutputT> implements
        TransformTranslator<ParDo.Bound<InputT, OutputT>> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ParDoBoundTranslator.class);

    @Override
    public void translateNode(ParDo.Bound<InputT, OutputT> transform, TranslationContext context) {
        DoFn<InputT, OutputT> doFn = transform.getFn();
        PCollection<OutputT> output = context.getOutput();
        WindowingStrategy<?, ?> windowingStrategy = output.getWindowingStrategy();

        DoFnBolt<InputT, OutputT> bolt = new DoFnBolt<>(context.getOptions(), doFn,
                windowingStrategy, new DefaultSideInputReader());

        PValue pvFrom = (PValue)context.getCurrentTransform().getInput();
        String from = baseName(pvFrom.getName());
        if(context.isGBKActive()){
            from = context.completeGBK();
        }
        LOG.info(baseName(pvFrom.getName()));

        PValue pvTo = (PValue)context.getCurrentTransform().getOutput();
        LOG.info(baseName(pvTo.getName()));
        String to = baseName(pvTo.getName());

        TranslationContext.Stream stream = new TranslationContext.Stream(from, to, new TranslationContext.Grouping(TranslationContext.Grouping.Type.SHUFFLE));

        context.addStream(stream);
        context.addBolt(baseName(pvTo.getName()), bolt);
    }

    private static String baseName(String str){
        return str.substring(0, str.lastIndexOf("."));
    }
}
