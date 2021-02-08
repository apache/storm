/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.jms.example;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * A simple <code>JmsTupleProducer</code> that expects to receive
 * JMS <code>TextMessage</code> objects with a body in JSON format.
 * <p/>
 * Ouputs a tuple with field name "json" and a string value
 * containing the raw json.
 * <p/>
 * <b>NOTE: </b> Currently this implementation assumes the text is valid
 * JSON and does not attempt to parse or validate it.
 * 
 * @author tgoetz
 *
 */
@SuppressWarnings("serial")
public class JsonTupleProducer implements JmsTupleProducer {

    @Override
    public Values toTuple(Message msg) throws JMSException {
        if (msg instanceof TextMessage) {
            String json = ((TextMessage) msg).getText();
            return new Values(json);
        } else {
            return null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

}
