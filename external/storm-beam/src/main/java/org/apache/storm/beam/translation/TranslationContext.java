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

import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.storm.beam.StormPipelineOptions;
import org.apache.storm.topology.IRichSpout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maintains the state necessary during Pipeline translation to build a Storm topology.
 */
public class TranslationContext {
    private StormPipelineOptions options;

    private TransformTreeNode currentTransform;

    private Map<String, IRichSpout> spoutMap = new HashMap<String, IRichSpout>();

    private Map<String, Object> boltMap = new HashMap<String, Object>();

    private List<Stream> streams = new ArrayList<Stream>();

    public TranslationContext(StormPipelineOptions options){
        this.options = options;

    }

    private String gbkTo = null;

    public StormPipelineOptions getOptions(){
        return this.options;
    }

    public void addSpout(String id, IRichSpout spout){
        this.spoutMap.put(id, spout);
    }

    public Map<String, IRichSpout> getSpouts(){
        return this.spoutMap;
    }

    public void addBolt(String id, Object bolt){
        this.boltMap.put(id, bolt);
    }

    public Object getBolt(String id){
        return this.boltMap.get(id);
    }

    public void addStream(Stream stream){
        this.streams.add(stream);
    }
    public List<Stream> getStreams(){
        return this.streams;
    }

    public void setCurrentTransform(TransformTreeNode transform){
        this.currentTransform = transform;
    }

    public TransformTreeNode getCurrentTransform(){
        return this.currentTransform;
    }

    public <InputT extends PInput> InputT getInput() {
        return (InputT) getCurrentTransform().getInput();
    }

    public <OutputT extends POutput> OutputT getOutput() {
        return (OutputT) getCurrentTransform().getOutput();
    }

    public void activateGBK(String gbkTo){
        this.gbkTo = gbkTo;
    }

    public String completeGBK(){
        String gbkTo = this.gbkTo;
        this.gbkTo = null;
        return gbkTo;
    }

    public boolean isGBKActive(){
        return this.gbkTo != null;
    }



    public static class Stream {

        private String from;
        private String to;
        private Grouping grouping;

        public Stream(String from, String to, Grouping grouping){
            this.from = from;
            this.to = to;
            this.grouping = grouping;
        }

        public String getTo() {
            return to;
        }

        public void setTo(String to) {
            this.to = to;
        }

        public String getFrom() {
            return from;
        }

        public void setFrom(String from) {
            this.from = from;
        }

        public Grouping getGrouping() {
            return grouping;
        }

        public void setGrouping(Grouping grouping) {
            this.grouping = grouping;
        }
    }

    public static class Grouping {

        /**
         * Types of stream groupings Storm allows
         */
        public static enum Type {
            ALL,
            CUSTOM,
            DIRECT,
            SHUFFLE,
            LOCAL_OR_SHUFFLE,
            FIELDS,
            GLOBAL,
            NONE
        }

        private Type type;
        private String streamId; // for named streams, other than DEFAULT
        private List<String> args; // arguments for fields grouping


        public Grouping(Type type){
            this.type = type;
        }

        public Grouping(List<String> args){
            this.type = Type.FIELDS;
            this.args = args;
        }
        public List<String> getArgs() {
            return args;
        }

        public void setArgs(List<String> args) {
            this.args = args;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public String getStreamId() {
            return streamId;
        }

        public void setStreamId(String streamId) {
            this.streamId = streamId;
        }

    }
}
