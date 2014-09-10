package com.alibaba.jstorm.message.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.TaskMessage;

import com.alibaba.jstorm.daemon.worker.metrics.JStormTimer;
import com.alibaba.jstorm.daemon.worker.metrics.Metrics;

public class MessageDecoder extends FrameDecoder {
	private static final Logger LOG = LoggerFactory
			.getLogger(MessageDecoder.class);


	private static JStormTimer timer = Metrics.registerTimer("netty-server-decode-timer");

	/*
	 * Each ControlMessage is encoded as: code (<0) ... short(2) Each
	 * TaskMessage is encoded as: task (>=0) ... short(2) len ... int(4) payload
	 * ... byte[] *
	 */
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buf) throws Exception {
		// Make sure that we have received at least a short
		long available = buf.readableBytes();
		if (available < 2) {
			// need more data
			return null;
		}


		timer.start();
		try {
			// Mark the current buffer position before reading task/len field
			// because the whole frame might not be in the buffer yet.
			// We will reset the buffer position to the marked position if
			// there's not enough bytes in the buffer.
			buf.markReaderIndex();

			// read the short field
			short code = buf.readShort();

			// case 1: Control message
			ControlMessage ctrl_msg = ControlMessage.mkMessage(code);
			if (ctrl_msg != null) {
				return ctrl_msg;
			}

			// case 2: task Message
			short task = code;

			// Make sure that we have received at least an integer (length)
			available -= 2;
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
			timer.stop();
		}

	}
}