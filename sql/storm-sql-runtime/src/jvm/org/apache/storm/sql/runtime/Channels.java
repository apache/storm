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
package org.apache.storm.sql.runtime;

import org.apache.storm.tuple.Values;

public class Channels {
  private static final ChannelContext VOID_CTX = new ChannelContext() {
    @Override
    public void emit(Values data) {}

    @Override
    public void fireChannelInactive() {}

    @Override
    public void flush() {

    }

    @Override
    public void setSource(java.lang.Object source) {

    }
  };

  private static class ChannelContextAdapter implements ChannelContext {
    private final ChannelHandler handler;
    private final ChannelContext next;

    public ChannelContextAdapter(
        ChannelContext next, ChannelHandler handler) {
      this.handler = handler;
      this.next = next;
    }

    @Override
    public void emit(Values data) {
      handler.dataReceived(next, data);
    }

    @Override
    public void fireChannelInactive() {
      handler.channelInactive(next);
    }

    @Override
    public void flush() {
      handler.flush(next);
    }

    @Override
    public void setSource(java.lang.Object source) {
      handler.setSource(next, source);
      next.setSource(source); // propagate through the chain
    }
  }

  private static class ForwardingChannelContext implements ChannelContext {
    private final ChannelContext next;

    public ForwardingChannelContext(ChannelContext next) {
      this.next = next;
    }

    @Override
    public void emit(Values data) {
      next.emit(data);
    }

    @Override
    public void fireChannelInactive() {
      next.fireChannelInactive();
    }

    @Override
    public void flush() {
      next.flush();
    }

    @Override
    public void setSource(Object source) {
      next.setSource(source);
    }
  }

  public static ChannelContext chain(
      ChannelContext next, ChannelHandler handler) {
    return new ChannelContextAdapter(next, handler);
  }

  public static ChannelContext voidContext() {
    return VOID_CTX;
  }
}
