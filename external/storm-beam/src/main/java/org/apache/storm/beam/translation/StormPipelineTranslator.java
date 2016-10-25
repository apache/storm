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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StormPipelineTranslator implements Pipeline.PipelineVisitor{
    private static final Logger LOG = LoggerFactory.getLogger(StormPipelineTranslator.class);
    private TranslationContext context;

    public StormPipelineTranslator(TranslationContext context){
        this.context = context;
    }


    public void translate(Pipeline pipeline) {
        pipeline.traverseTopologically(this);
    }

    public CompositeBehavior enterCompositeTransform(TransformTreeNode transformTreeNode) {
        LOG.info("entering composite translation {}", transformTreeNode.getTransform());
        return CompositeBehavior.ENTER_TRANSFORM;
    }

    public void leaveCompositeTransform(TransformTreeNode transformTreeNode) {
        LOG.info("leaving composite translation {}", transformTreeNode.getTransform());
    }

    public void visitPrimitiveTransform(TransformTreeNode transformTreeNode) {
        LOG.info("visiting transform {}", transformTreeNode.getTransform());
        PTransform transform = transformTreeNode.getTransform();
        LOG.info("class: {}", transform.getClass());
        TransformTranslator translator = TranslatorRegistry.getTranslator(transform);
        if(translator != null) {
            context.setCurrentTransform(transformTreeNode);
            translator.translateNode(transformTreeNode.getTransform(), context);
        } else {
            LOG.warn("No translator found for {}", transform.getClass());
        }
    }

    public void visitValue(PValue value, TransformTreeNode transformTreeNode) {
        LOG.info("visiting value {}", value);
    }
}
