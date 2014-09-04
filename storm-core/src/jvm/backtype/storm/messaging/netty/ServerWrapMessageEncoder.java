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

import javax.security.sasl.SaslException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerWrapMessageEncoder extends OneToOneEncoder {

	@Override
	protected Object encode(ChannelHandlerContext ctx, Channel ch,
			Object obj) throws Exception {
		Channel channel = ctx.getChannel();

		SaslNettyServer saslNettyServer = SaslNettyServerState.getSaslNettyServer
				.get(channel);

		// Check if we need to wrap the message pay load.
		boolean isWrap = isUseUnwrap(saslNettyServer);
		if (!isWrap) {
			return obj;
		}

		// Wrapping message pay load
		byte[] messagePayLoad = null;
		if (obj instanceof ChannelBuffer) {
			final ChannelBuffer buf = (ChannelBuffer) obj;
			final int length = buf.readableBytes();
			messagePayLoad = new byte[length];
			buf.markReaderIndex();
			buf.readBytes(messagePayLoad);
		}

		byte[] wrappedPayLoad = null;

		try {
			wrappedPayLoad = saslNettyServer.wrap(messagePayLoad, 0,
					messagePayLoad.length);
		} catch (final SaslException se) {
			try {
				saslNettyServer.dispose();
			} catch (final SaslException igonored) {

			}
			throw se;
		}

		ChannelBufferOutputStream bout = null;

		if (wrappedPayLoad != null) {
			bout = new ChannelBufferOutputStream(
					ChannelBuffers.directBuffer(wrappedPayLoad.length + 4));
			bout.writeInt(wrappedPayLoad.length);
			if (wrappedPayLoad.length > 0) {
				bout.write(wrappedPayLoad);
			}
			bout.close();
			wrappedPayLoad = null;
		}
		return bout.buffer();
	}

}
