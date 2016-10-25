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

import avro.shaded.com.google.common.collect.Lists;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PValue;
import org.apache.storm.beam.translation.runtime.GroupByKeyCompleteBolt;
import org.apache.storm.beam.translation.runtime.GroupByKeyInitBolt;

import java.util.List;

/**
 * Translates a Beam GroupByKey operation into a pair of Storm Bolts with a fields grouping.
 *
 * TODO: From a Beam perspective this is likely the wrong approach to doing GBK
 */
public class GroupByKeyTranslator<K, V> implements
        TransformTranslator<GroupByKey<K, V>> {
    @Override
    public void translateNode(GroupByKey<K, V> transform, TranslationContext context) {
        PValue pvFrom = (PValue)context.getCurrentTransform().getInput();

        PValue pvTo = (PValue)context.getCurrentTransform().getEnclosingNode().getOutput();

        String from = baseName(pvFrom.getName());
        String to = baseName(pvTo.getName());
        context.activateGBK(to);
        String initBolt = from + "_GBK_init"; // first GBK bolt
        String completeBolt = from + "_GBK_complete";

        GroupByKeyInitBolt gbkInit = new GroupByKeyInitBolt();
        GroupByKeyCompleteBolt gbkComplete = new GroupByKeyCompleteBolt();


        // from --> initBolt
        TranslationContext.Stream stream = new TranslationContext.Stream(from, initBolt, new TranslationContext.Grouping(TranslationContext.Grouping.Type.SHUFFLE));
        context.addStream(stream);
        context.addBolt(initBolt, gbkInit);

        // initBolt --> completeBolt
        TranslationContext.Grouping fieldsGrouping = new TranslationContext.Grouping(TranslationContext.Grouping.Type.FIELDS);
        List fields = Lists.newArrayList();
        fields.add("keyValue");
        fieldsGrouping.setArgs(fields);
        context.addBolt(completeBolt, gbkComplete);
        stream = new TranslationContext.Stream(initBolt, completeBolt, fieldsGrouping);
        context.addStream(stream);

        // completeBolt --> to
        stream = new TranslationContext.Stream(completeBolt, to, new TranslationContext.Grouping(TranslationContext.Grouping.Type.SHUFFLE));
        context.addStream(stream);
    }


    private static String baseName(String str){
        return str.substring(0, str.lastIndexOf("."));
    }
}
