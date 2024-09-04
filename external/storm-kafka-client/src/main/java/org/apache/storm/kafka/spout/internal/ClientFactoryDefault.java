/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout.internal;

import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ClientFactoryDefault<K, V> implements ClientFactory<K, V> {

    @Override
    public KafkaConsumer<K, V> createConsumer(Map<String, Object> consumerProps) {
        return new KafkaConsumer<>(consumerProps);
    }

    @Override
    public Admin createAdmin(Map<String, Object> adminProps) {
        return Admin.create(adminProps);
    }

}
