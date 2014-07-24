package com.alibaba.jstorm.message.zeroMq;

import java.util.List;

import org.zeromq.ZMQ.Socket;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.daemon.worker.metrics.JStormHistogram;
import com.alibaba.jstorm.daemon.worker.metrics.JStormTimer;
import com.alibaba.jstorm.daemon.worker.metrics.Metrics;
import com.alibaba.jstorm.utils.JStormServerUtils;

/**
 * 
 * @author longda
 * 
 */
public class ZMQSendConnection implements IConnection {
	private org.zeromq.ZMQ.Socket socket;
	private boolean closed = false;
	private JStormTimer timer;
	private JStormHistogram histogram;

	public ZMQSendConnection(Socket _socket, String host, int port) {
		socket = _socket;
		timer = Metrics.registerTimer(JStormServerUtils.getName(host, port)
				+ "-zmq-send-timer");
		histogram = Metrics.registerHistograms(JStormServerUtils.getName(host,
				port) + "-zmq-send-histogram");
	}

	@Override
	public void close() {
		socket.close();
		closed = true;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public void registerQueue(DisruptorQueue recvQueu) {
		throw new UnsupportedOperationException(
				"recvTask() Client connection should not receive any messages");
	}

	@Override
	public void enqueue(TaskMessage message) {
		throw new UnsupportedOperationException(
				"recvTask() Client connection should not receive any messages");
	}

	@Override
	public void send(List<TaskMessage> messages) {
		timer.start();

		try {
			for (TaskMessage message : messages) {
				ZeroMq.send(socket, message.message());
			}
		} finally {
			timer.stop();
			histogram.update(messages.size());
		}
	}

	@Override
	public void send(TaskMessage message) {
		timer.start();
		try {
			ZeroMq.send(socket, message.message());
		} finally {
			timer.stop();
			histogram.update(1);
		}
	}

	@Override
	public TaskMessage recv(int flags) {
		throw new UnsupportedOperationException(
				"recvTask() Client connection should not receive any messages");
	}

}
