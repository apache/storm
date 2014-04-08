package com.alibaba.jstorm.task.comm;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RunCounter;

//import com.alibaba.jstorm.message.zeroMq.ISendConnection;

/**
 * Message dispatcher
 * 
 * @author yannian/Longda
 * 
 */
public class VirtualPortDispatch extends RunnableCallback {
	private final static Logger LOG = Logger
			.getLogger(VirtualPortDispatch.class);

	private String topologyId;
	private IContext context;
	private IConnection recvConn;
	private Set<Integer> valid_ports = null;
	private Map<Integer, IConnection> sendConns;
	private AtomicBoolean active;

	private RunCounter runCounter = new RunCounter("VirtualPortDispatch",
			VirtualPortDispatch.class);

	public VirtualPortDispatch(String topologyId, IContext context,
			IConnection recvConn, Set<Integer> valid_ports,
			AtomicBoolean active) {
		this.topologyId = topologyId;
		this.context = context;
		this.recvConn = recvConn;
		this.valid_ports = valid_ports;
		this.active = active;
		
		this.sendConns = new HashMap<Integer, IConnection>();
	}

	public void cleanup() {
		LOG.info("Virtual port  received shutdown notice");
		byte shutdownCmd[] = { TaskStatus.SHUTDOWN };
		for (Entry<Integer, IConnection> entry : sendConns.entrySet()) {
			int taskId = entry.getKey();
			IConnection sendConn = entry.getValue();
			sendConn.send(taskId, shutdownCmd);
			sendConn.close();
		}

		recvConn.close();

		recvConn = null;
	}

	@Override
	public void run() {
		boolean hasTuple = false;

		while (active.get() == true) {
			byte[] data = recvConn.recv(0);
			if (data == null || data.length == 0) {
				if (hasTuple == false) {
					JStormUtils.sleepMs(1);
				}
				continue;
			}
			hasTuple = true;

			long before = System.currentTimeMillis();

			int port = KryoTupleDeserializer.deserializeTaskId(data);

			if (port == -1) {
				// shutdown message
				active.set(false);

				break;
			}

			// LOG.info("Get message to port " + port);
			if (valid_ports == null || valid_ports.contains(port)) {
				IConnection sendConn = sendConns.get(port);
				if (sendConn == null) {
					sendConn = context.connect(topologyId, "localhost", port,
							false);
					sendConns.put(port, sendConn);
				}
				sendConn.send(port, data);

			} else {
				LOG.warn("Received invalid message directed at port " + port
						+ ". Dropping...");
			}

			long after = System.currentTimeMillis();
			runCounter.count(after - before);
		}
		
		if (active.get() == false) {
			cleanup();
		}

	}

	@Override
	public Object getResult() {
		if (recvConn == null) {
			return -1;
		} else if (active.get() == false ) {
			if (recvConn != null) {
				cleanup();
			}
			
			return -1;
		}else {
			return 0;
		}
	}
}
