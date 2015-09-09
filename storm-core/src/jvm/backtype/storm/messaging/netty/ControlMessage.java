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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;

enum ControlMessage {
    CLOSE_MESSAGE((short)-100),
    EOB_MESSAGE((short)-201),
    OK_RESPONSE((short)-200),
    FAILURE_RESPONSE((short)-400),
    SASL_TOKEN_MESSAGE_REQUEST((short)-202),
    SASL_COMPLETE_REQUEST((short)-203);

    private short code;

    //private constructor
    private ControlMessage(short code) {
        this.code = code;
    }

    /**
     * Return a control message per an encoded status code
     * @param encoded
     * @return
     */
    static ControlMessage mkMessage(short encoded) {
        for(ControlMessage cm: ControlMessage.values()) {
          if(encoded == cm.code) return cm;
        }
        return null;
    }

    int encodeLength() {
        return 2; //short
    }
    
    /**
     * encode the current Control Message into a channel buffer
     * @throws Exception if failed to write to buffer
     */
    ByteBuf buffer() throws IOException {
        ByteBufOutputStream bout = new ByteBufOutputStream(Unpooled.directBuffer(encodeLength()));
        write(bout);
        bout.close();
        return bout.buffer();
    }

    void write(ByteBufOutputStream bout) throws IOException {
        bout.writeShort(code);        
    } 
}
