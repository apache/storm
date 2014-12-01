package com.alibaba.jstorm.message.netty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.net.SocketAddress;
import java.net.InetSocketAddress;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.TaskMessage;

import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.JStormHistogram;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.Metrics;

public class MessageDecoder extends FrameDecoder {
	private static final Logger LOG = LoggerFactory
			.getLogger(MessageDecoder.class);

	private static JStormTimer timer = null;
	private static Map<String, JStormHistogram> networkTransmitTimeMap = null;
	
	public MessageDecoder(boolean isServer) {
		if (isServer) {
		    if (timer == null)
		        timer = Metrics.registerTimer(null, MetricDef.NETTY_SERV_DECODE_TIME, 
				        null, Metrics.MetricType.WORKER);
		}
		
		if (networkTransmitTimeMap == null)
		    networkTransmitTimeMap = new HashMap<String, JStormHistogram>();
	}

	/*
	 * Each ControlMessage is encoded as: code (<0) ... short(2) Each
	 * TaskMessage is encoded as: task (>=0) ... short(2) len ... int(4) payload
	 * ... byte[] *
	 */
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buf) throws Exception {		
		// Make sure that we have received at least a short
		long available = buf.readableBytes();
		// Length of control message is 10. 
		// Minimum length of a task message is 6(short taskId, int length).
		if (available < 6) {
			// need more data
			return null;
		}


		if (timer != null) timer.start();
		try {
			// Mark the current buffer position before reading task/len field
			// because the whole frame might not be in the buffer yet.
			// We will reset the buffer position to the marked position if
			// there's not enough bytes in the buffer.
			buf.markReaderIndex();

				// read the short field
				short code = buf.readShort();
				available -= 2;

				// case 1: Control message
				ControlMessage ctrl_msg = ControlMessage.mkMessage(code);
				if (ctrl_msg != null) {
					if (available < 8) {
						// The time stamp bytes were not received yet - return null.
						buf.resetReaderIndex();
						return null;
					}					
                    long timeStamp = buf.readLong();
                    available -= 8;
					if (ctrl_msg == ControlMessage.EOB_MESSAGE) {
						InetSocketAddress sockAddr = (InetSocketAddress)(channel.getRemoteAddress());
					    String remoteAddr = sockAddr.getHostName() + ":" + sockAddr.getPort();
						
					    long interval = System.currentTimeMillis() - timeStamp;
					    if (interval < 0) interval = 0;

					    JStormHistogram netTransTime = networkTransmitTimeMap.get(remoteAddr);
					    if (netTransTime == null) {
					    	netTransTime = Metrics.registerHistograms(remoteAddr, MetricDef.NETWORK_MSG_TRANS_TIME, 
					    			null, Metrics.MetricType.WORKER);
					    	networkTransmitTimeMap.put(remoteAddr, netTransTime);
					    }
					    
					    netTransTime.update(interval);
					} 
					
					return ctrl_msg;
				}

				// case 2: task Message
				short task = code;

				// Make sure that we have received at least an integer (length)
				if (available < 4) {
					// need more data
					buf.resetReaderIndex();

				    return null;
				}

			// Read the length field.
			int length = buf.readInt();
			if (length <= 0) {
				LOG.info("Receive one message whose TaskMessage's message length is {}", length);
				return new TaskMessage(task, null);
			}

			// Make sure if there's enough bytes in the buffer.
			available -= 4;
			if (available < length) {
				// The whole bytes were not received yet - return null.
				buf.resetReaderIndex();

				return null;
			}

			// There's enough bytes in the buffer. Read it.
			ChannelBuffer payload = buf.readBytes(length);

			// Successfully decoded a frame.
			// Return a TaskMessage object

			byte[] rawBytes = payload.array();
			// @@@ TESTING CODE
			// LOG.info("Receive task:{}, length: {}, data:{}",
			// task, length, JStormUtils.toPrintableString(rawBytes));

			TaskMessage ret = new TaskMessage(task, rawBytes);

			return ret;
		} finally {
			if (timer != null) timer.stop();
		}

	}
}