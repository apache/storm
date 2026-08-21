/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Random;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * A {@link Random} that returns slices of an AES counter mode key stream, keyed from {@link SecureRandom} when the instance is
 * created and buffered a block at a time. It is meant for values that other parties must not be able to guess, such as the tuple
 * tree ids handed out by {@link org.apache.storm.tuple.MessageId#generateId(Random)}: unlike {@link Random},
 * {@link java.util.SplittableRandom} and {@link java.util.concurrent.ThreadLocalRandom}, whose internal state is recoverable from
 * a couple of returned values, the values returned here say nothing about the values returned next.
 *
 * <p>The buffering keeps it cheap enough for the emit path; it is measurably faster than {@link Random}, whose seed update is a
 * contended compare and set per value. It is not a drop in replacement for a seeded {@link Random} though, because it cannot be
 * reseeded and so cannot produce a repeatable sequence.</p>
 */
public class KeyStreamRandom extends Random {
    private static final long serialVersionUID = 1L;
    private static final String TRANSFORMATION = "AES/CTR/NoPadding";
    private static final int KEY_BYTES = 16;
    private static final int DEFAULT_BUFFER_LONGS = 512;

    private final SecureRandom source = new SecureRandom();
    private final Cipher cipher;
    private final byte[] input;
    private final byte[] output;
    private final long[] buffer;
    private int position;

    public KeyStreamRandom() {
        this(DEFAULT_BUFFER_LONGS);
    }

    KeyStreamRandom(int bufferLongs) {
        this.cipher = newCipher(source);
        this.input = new byte[bufferLongs * Long.BYTES];
        this.output = new byte[bufferLongs * Long.BYTES];
        this.buffer = new long[bufferLongs];
        // start empty, the first call fills the buffer
        this.position = bufferLongs;
    }

    private static Cipher newCipher(SecureRandom source) {
        byte[] key = new byte[KEY_BYTES];
        byte[] iv = new byte[KEY_BYTES];
        source.nextBytes(key);
        source.nextBytes(iv);
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(iv));
            return cipher;
        } catch (GeneralSecurityException e) {
            // no AES here, fall back to taking the values from the SecureRandom itself
            return null;
        }
    }

    @Override
    public synchronized long nextLong() {
        if (position == buffer.length) {
            fill();
        }
        return buffer[position++];
    }

    @Override
    protected int next(int bits) {
        return (int) (nextLong() >>> (Long.SIZE - bits));
    }

    @Override
    public void setSeed(long seed) {
        // there is nothing to seed, and Random's constructor calls this before this class' fields exist
    }

    private void fill() {
        if (cipher == null) {
            source.nextBytes(output);
        } else {
            try {
                cipher.update(input, 0, input.length, output, 0);
            } catch (GeneralSecurityException e) {
                throw Utils.wrapInRuntime(e);
            }
        }
        ByteBuffer.wrap(output).order(ByteOrder.nativeOrder()).asLongBuffer().get(buffer);
        position = 0;
    }
}
