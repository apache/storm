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
package backtype.storm.messaging.netty;

import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.List;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormClientHandler extends SimpleChannelUpstreamHandler  {
    private static final Logger LOG = LoggerFactory.getLogger(StormClientHandler.class);
    private Client client;
    
    StormClientHandler(Client client) {
        this.client = client;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        //examine the response message from server
        Object message = event.getMessage();
        if (message instanceof ControlMessage) {
          ControlMessage msg = (ControlMessage)message;
          if (msg==ControlMessage.FAILURE_RESPONSE) {
              LOG.info("failure response:{}", msg);
          }
          //All others are ignored
        } else if (message instanceof List) {
          //This should be the metrics, and there should only be one of them
          List<TaskMessage> list = (List<TaskMessage>)message;
          if (list.size() != 1) throw new RuntimeException("Expected to only see one message for load metrics");
          TaskMessage tm = ((List<TaskMessage>)message).get(0);
          if (tm.task() != -1) throw new RuntimeException("Metrics messages are sent to the system task");
          Object metrics = Utils.deserialize(tm.message());
          if (!(metrics instanceof Map)) throw new RuntimeException("metrics are expected to be a map");
          client.setLoadMetrics((Map<Integer, Double>)metrics);
        } else {
          throw new RuntimeException("Don't know how to handle a message of type "+message);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
        Throwable cause = event.getCause();
        if (!(cause instanceof ConnectException)) {
            LOG.info("Connection to "+client.name()+" failed", cause);
        }
    }
}
