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
package org.apache.storm.multilang;

import static org.msgpack.template.Templates.TString;
import static org.msgpack.template.Templates.TValue;
import static org.msgpack.template.Templates.tMap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.template.Template;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Unpacker;

/**
 * MssagePackSerializer implements the multilang protocol.
 * <br/>
 * MessagePack is an efficient binary serialization format.
 * It lets you exchange data among multiple languages like JSON. But it's faster and smaller. 
 * 
 */
public class MessagePackSerializer implements ISerializer {
    //ANY CHANGE TO THIS CODE MUST BE SERIALIZABLE COMPATIBLE OR THERE WILL BE PROBLEMS
    private static final long serialVersionUID = 3449113168794677765L;

    private DataOutputStream processIn;

    private Packer packer;
    private Unpacker unpacker;

    private Template<Map<String,Value>> mapTmpl;

    public void initialize(OutputStream processIn, InputStream processOut) {
        this.processIn = new DataOutputStream(processIn);
        MessagePack msgPack = new MessagePack();
        this.packer = msgPack.createPacker(this.processIn);
        this.unpacker = msgPack.createUnpacker(processOut);
        this.mapTmpl = tMap(TString, TValue);
    }

    public Number connect(Map conf, TopologyContext context)
            throws IOException, NoOutputException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("pidDir", context.getPIDDir());
        map.put("conf", conf);

        //get map from context object
        Map<String, Object> contextMap = new HashMap<String, Object>();
        contextMap.put("task->component", context.getTaskToComponent());
        contextMap.put("taskid", context.getThisTaskId());
        contextMap.put("componentid", context.getThisComponentId());
        map.put("context", contextMap);

        writeMessage(map);

        return readMessage().get("pid").asIntegerValue().getInt();
    }

    public void writeBoltMsg(BoltMsg msg) throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("id", msg.getId());
        map.put("comp", msg.getComp());
        map.put("stream", msg.getStream());
        map.put("task", msg.getTask());
        map.put("tuple", msg.getTuple());
        writeMessage(map);
    }

    public void writeSpoutMsg(SpoutMsg msg) throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("command", msg.getCommand());
        map.put("id", msg.getId());
        writeMessage(map);
    }

    public void writeTaskIds(List<Integer> taskIds) throws IOException {
        writeMessage(taskIds);
    }

    private void writeMessage(Object msg) throws IOException {
        this.packer.write(msg);
        this.processIn.flush();
    }

    public ShellMsg readShellMsg() throws IOException, NoOutputException {
        Map<String, Value> msg = readMessage();
        ShellMsg shellMsg = new ShellMsg();

        //String command
        String command = msg.get("command").asRawValue().getString();
        shellMsg.setCommand(command);

        //Object id
        Object id = null;
        Value idValue = msg.get("id");
        if (idValue != null) {
            //convert to string when using number id
            if (idValue.isIntegerValue()) {
                id = msg.get("id").asIntegerValue().toString();
            } else {
                id = idValue.asRawValue().getString();
            }
        }
        shellMsg.setId(id);

        //String msg
        Value log = msg.get("msg");
        if(log != null) {
            shellMsg.setMsg(log.asRawValue().getString());
        }

        //String stream
        String stream = Utils.DEFAULT_STREAM_ID;
        Value streamValue = msg.get("stream");
        if (streamValue != null) {
            stream = streamValue.asRawValue().getString();
        }
        shellMsg.setStream(stream);

        //long task
        Value taskValue = msg.get("task");
        if (taskValue != null) {
            shellMsg.setTask(taskValue.asIntegerValue().getLong());
        } else {
            shellMsg.setTask(0);
        }

        //boolean needTaskIds
        Value need_task_ids = msg.get("need_task_ids");
        if (need_task_ids == null || need_task_ids.asBooleanValue().getBoolean()) {
            shellMsg.setNeedTaskIds(true);
        } else {
            shellMsg.setNeedTaskIds(false);
        }

        //List<Object> tuple
        Value tupleValue = msg.get("tuple");
        if (tupleValue != null) {
            for (Value v : tupleValue.asArrayValue()) {
                shellMsg.addTuple(toJavaType(v));
            }
        }

        //List<String> anchors
        Value anchorsValue = msg.get("anchors");
        if(anchorsValue != null) {
            for (Value v : anchorsValue.asArrayValue()) {
                shellMsg.addAnchor(v.asRawValue().getString());
            }
        }

        //String metricName
        Value nameValue = msg.get("name");
        if(nameValue != null) {
            shellMsg.setMetricName(nameValue.asRawValue().getString());
        }

        //Object metricParams
        Value paramsValue = msg.get("params");
        if(paramsValue != null) {
            shellMsg.setMetricParams(paramsValue.asRawValue().getString());
        }

        //ShellLogLevel logLevel
        if (command.equals("log")) {
            Value logLevelValue = msg.get("level");
            if (logLevelValue != null) {
                shellMsg.setLogLevel(logLevelValue.asIntegerValue().getInt());
            }
        }

        return shellMsg;
    }

    private Map<String, Value> readMessage() throws IOException {
        return this.unpacker.read(this.mapTmpl);
    }

    private Object toJavaType(Value value) {
        switch (value.getType()) {
            case NIL:
                return null;
            case BOOLEAN:
                return value.asBooleanValue().getBoolean();
            case INTEGER:
                return value.asIntegerValue().getLong();
            case FLOAT:
                return value.asFloatValue().getDouble();
            case ARRAY:
                List<Object> list = new ArrayList<Object>();
                for (Value v : value.asArrayValue().getElementArray()) {
                    list.add(toJavaType(v));
                }
                return list;
            case MAP:
                Map<Object,Object> map = new HashMap<Object,Object>();
                for (Map.Entry<Value, Value> entry : value.asMapValue().entrySet()) {
                    map.put(toJavaType(entry.getKey()), toJavaType(entry.getValue()));
                }
                return map;
            case RAW:
                return value.asRawValue().getString();
            default:
                return value;
        }
    }
}
