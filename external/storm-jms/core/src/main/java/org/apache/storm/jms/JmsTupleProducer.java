/**
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
package org.apache.storm.jms;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;

/**
 * Interface to define classes that can produce a Storm <code>Values</code> objects
 * from a <code>javax.jms.Message</code> object>.
 * <p>
 * Implementations are also responsible for declaring the output
 * fields they produce.
 * <p>
 * If for some reason the implementation can't process a message
 * (for example if it received a <code>javax.jms.ObjectMessage</code>
 * when it was expecting a <code>javax.jms.TextMessage</code> it should
 * return <code>null</code> to indicate to the <code>JmsSpout</code> that
 * the message could not be processed.
 *
 */
public interface JmsTupleProducer extends Serializable {
    /**
     * Process a JMS message object to create a Values object.
     *
     * @param msg - the JMS message
     * @return the Values tuple, or null if the message couldn't be processed.
     * @throws JMSException
     */
    Values toTuple(Message msg) throws JMSException;

    /**
     * Declare the output fields produced by this JmsTupleProducer.
     *
     * @param declarer The OuputFieldsDeclarer for the spout.
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
}
