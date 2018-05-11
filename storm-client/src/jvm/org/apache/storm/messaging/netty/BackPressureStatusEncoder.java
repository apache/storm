/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.messaging.netty;

import java.util.List;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.apache.storm.shade.io.netty.handler.codec.MessageToMessageEncoder;

public class BackPressureStatusEncoder extends MessageToMessageEncoder<BackPressureStatus> {

    private final KryoValuesSerializer ser;

    public BackPressureStatusEncoder(KryoValuesSerializer ser) {
        this.ser = ser;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, BackPressureStatus msg, List<Object> out) throws Exception {
        out.add(msg.buffer(ctx.alloc(), ser));
    }
    
}
