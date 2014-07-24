package com.alibaba.jstorm.message.netty;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.TaskMessage;

import com.alibaba.jstorm.daemon.worker.metrics.JStormHistogram;
import com.alibaba.jstorm.daemon.worker.metrics.JStormTimer;
import com.alibaba.jstorm.daemon.worker.metrics.Metrics;

public class MessageDecoder extends FrameDecoder {
	private static final Logger LOG = LoggerFactory
			.getLogger(MessageDecoder.class);

	private static final int BATCH_SIZE = 32;

	private static JStormTimer timer = Metrics.registerTimer("netty-server-decode-timer");
	private static JStormHistogram histogram = Metrics
			.registerHistograms("netty-server-decode-histogram");

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

		List<Object> ret = new ArrayList<Object>();

		timer.start();;
		try {
			int decodeNum = 0;
			while (available >= 2 && decodeNum < BATCH_SIZE) {
				// Mark the current buffer position before reading task/len
				// field
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

					if (ctrl_msg == ControlMessage.EOB_MESSAGE) {
						continue;
					} else {
						LOG.warn("Occur invalid control message {}", ctrl_msg);
						break;
					}
				}

				// case 2: task Message
				short task = code;

				// Make sure that we have received at least an integer (length)
				if (available < 4) {
					// need more data
					buf.resetReaderIndex();
					break;
				}

				// Read the length field.
				int length = buf.readInt();
				available -= 4;
				if (length <= 0) {
					// skip add new empty TaskMessage
					// ret.add(new TaskMessage(task, null));
					LOG.info("Receive one empty TaskMessage");
					break;
				}

				// Make sure if there's enough bytes in the buffer.
				if (available < length) {
					// The whole bytes were not received yet - return null.
					buf.resetReaderIndex();
					break;
				}
				available -= length;

				// There's enough bytes in the buffer. Read it.
				ChannelBuffer payload = buf.readBytes(length);

				// Successfully decoded a frame.
				// Return a TaskMessage object

				ret.add(new TaskMessage(task, payload.array()));
				decodeNum++;
				// @@@ TESTING CODE
				// LOG.info("Receive task:{}, length: {}, data:{}",
				// task, length, JStormUtils.toPrintableString(rawBytes));
			}
		} finally {
			timer.stop();
			histogram.update(ret.size());
		}

		if (ret.size() == 0) {
			return null;
		} else {
			// LOG.info("TaskMessage List size: {}", ret.size());
			return ret;
		}
	}
}