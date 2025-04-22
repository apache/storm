/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.jms;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import java.io.Serializable;

import org.apache.storm.tuple.ITuple;

/**
 * JmsMessageProducer implementations are responsible for translating
 * a <code>org.apache.storm.tuple.Values</code> instance into a
 * <code>jakarta.jms.Message</code> object.
 */
public interface JmsMessageProducer extends Serializable {

    /**
     * Translate a <code>org.apache.storm.tuple.Tuple</code> object
     * to a <code>jakarta.jms.Message</code> object.
     */
    Message toMessage(Session session, ITuple input) throws JMSException;
}
