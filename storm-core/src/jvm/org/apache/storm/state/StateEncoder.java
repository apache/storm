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

package org.apache.storm.state;

/**
 * The interface of State Encoder.
 */
public interface StateEncoder<K, V, KENCODED, VENCODED> {
    /**
     * Encode key.
     *
     * @param key the value of key (K type)
     * @return the encoded value of key (KENCODED type)
     */
    KENCODED encodeKey(K key);

    /**
     * Encode value.
     *
     * @param value the value of value (V type)
     * @return the encoded value of value (VENCODED type)
     */
    VENCODED encodeValue(V value);

    /**
     * Decode key.
     *
     * @param encodedKey the value of key (KRAW type)
     * @return the decoded value of key (K type)
     */
    K decodeKey(KENCODED encodedKey);

    /**
     * Decode value.
     *
     * @param encodedValue the value of key (VENCODED type)
     * @return the decoded value of key (V type)
     */
    V decodeValue(VENCODED encodedValue);

    /**
     * Get the tombstone value (deletion mark).
     *
     * @return the tomestone value (VENCODED type)
     */
    VENCODED getTombstoneValue();
}
