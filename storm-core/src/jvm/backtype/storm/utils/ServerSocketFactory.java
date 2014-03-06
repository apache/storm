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
package backtype.storm.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class ServerSocketFactory {
    private boolean reuseAddress = false;
    private Map<Integer, ServerSocket> sockets = new HashMap<Integer, ServerSocket>();

    public ServerSocketFactory(boolean reusePort) {
        this.reuseAddress = reusePort;
    }

    public ServerSocket create() throws IOException {
        ServerSocket socket = new ServerSocket();
        socket.setReuseAddress(reuseAddress);
        socket.bind(null);
        int port = socket.getLocalPort();
        sockets.put(port, socket);
        return socket;
    }

    public void free(int port) throws IOException {
        ServerSocket socket = sockets.get(port);
        if (null != socket) {
            socket.close();
            sockets.remove(port);
        }
    }
}
