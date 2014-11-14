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

import static backtype.storm.messaging.netty.SaslUtils.isUseUnwrap;

import java.util.ArrayList;
import java.util.List;

import javax.security.sasl.SaslException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import backtype.storm.messaging.TaskMessage;

public class ServerUnwrapMessageDecoder extends FrameDecoder {

	@Override
	protected Object decode(final ChannelHandlerContext ctx, final Channel ch,
			ChannelBuffer buf) throws Exception {

		final SaslNettyServer saslNettyServer = SaslNettyServerState.getSaslNettyServer
				.get(ch);
		
		// Check if we need to unwrap the message pay load.
		final boolean isUnwrap = isUseUnwrap(saslNettyServer);
		
		// Unwrapping message pay load
		if (isUnwrap) {

			// Make sure that we have received at least a int
			long available = buf.readableBytes();
			if (available < 4) {
				// need more data
				return null;
			}

			if (available >= 4) {
				buf.markReaderIndex();

				final int wrappedPayLoadLength = buf.readInt();
				available -= 4;

				if (wrappedPayLoadLength <= 0) {
					return buf;
				}

				if (available < wrappedPayLoadLength) {
					// The whole bytes were not received yet - return null
					buf.resetReaderIndex();
					return null;
				}

				byte[] unWrappedPayLoad = null;
				try {
					unWrappedPayLoad = saslNettyServer.unwrap(
							buf.readBytes(wrappedPayLoadLength).array(), 0,
							wrappedPayLoadLength);
				} catch (final SaslException se) {
					try {
						saslNettyServer.dispose();
					} catch (final SaslException ignored) {
					}
					throw se;
				}

				buf = ChannelBuffers.dynamicBuffer();
				buf.writeBytes(unWrappedPayLoad);
			}
		}

		// Make sure that we have received at least a short
		long available = buf.readableBytes();
		if (available < 2) {
			// need more data
			return null;
		}

		List<Object> ret = new ArrayList<Object>();

		// Use while loop, try to decode as more messages as possible in single
		// call
		while (available >= 2) {

			// Mark the current buffer position before reading task/len field
			// because the whole frame might not be in the buffer yet.
			// We will reset the buffer position to the marked position if
			// there's not enough bytes in the buffer.
			buf.markReaderIndex();

			// read the short field
			short code = buf.readShort();
			available -= 2;

			// Preparing control message
			ControlMessage ctrl_msg = ControlMessage.mkMessage(code);

			// case 1: Sasl Token Message Request or Sasl Complete Message
			// Request
			if (ctrl_msg != null
					&& (ctrl_msg == ControlMessage.SASL_TOKEN_MESSAGE_REQUEST || ctrl_msg == ControlMessage.SASL_COMPLETE_REQUEST)) {
				return ctrl_msg;
			}

			// case 2: SaslTokenMessageRequest
			if (ctrl_msg != null
					&& ctrl_msg == ControlMessage.SASL_TOKEN_MESSAGE) {
				// Make sure that we have received at least an integer (length)
				if (available < 4) {
					// need more data
					buf.resetReaderIndex();
					return null;
				}

				// Read the length field.
				int payLoadLength = buf.readInt();
				available -= 4;
				if (payLoadLength <= 0) {
					return new SaslMessageToken(null);
				}

				// Make sure if there's enough bytes in the buffer.
				if (available < payLoadLength) {
					// The whole bytes were not received yet - return null.
					buf.resetReaderIndex();
					return null;
				}

				// There's enough bytes in the buffer. Read it.
				ChannelBuffer payload = buf.readBytes(payLoadLength);

				// Successfully decoded a frame.
				// Return a SaslTokenMessageRequest object
				return new SaslMessageToken(payload.array());
			}

			// case 3: End of Message Control Message
			if (ctrl_msg != null) {

				if (ctrl_msg == ControlMessage.EOB_MESSAGE) {
					continue;
				} else {
					return ctrl_msg;
				}
			}

			// case 4: Task Message
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
				ret.add(new TaskMessage(task, null));
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
		}

		if (ret.size() == 0) {
			return null;
		} else {
			return ret;
		}
	}

}
