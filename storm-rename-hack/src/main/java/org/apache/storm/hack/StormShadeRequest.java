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

package org.apache.storm.hack;

import org.apache.storm.hack.relocation.Relocator;
import org.apache.storm.hack.relocation.SimpleRelocator;
import org.apache.storm.hack.resource.ClojureTransformer;
import org.apache.storm.hack.resource.ResourceTransformer;

import java.util.Arrays;

public class StormShadeRequest {
    public static ShadeRequest makeRequest() {
        ShadeRequest request = new ShadeRequest();
        String oldNamespaceWarnMessage = "please modify your code to use the new namespace";
        String internalClassAccessMessage = "You are accessing an internal class of storm which can be removed in future. "
                                            + "Do not use the shaded classes inside storm";
        request.setRelocators(Arrays.asList(
                (Relocator)new SimpleRelocator("backtype.storm", "org.apache.storm", oldNamespaceWarnMessage),
                (Relocator)new SimpleRelocator("storm.trident", "org.apache.storm.trident", oldNamespaceWarnMessage),
                (Relocator)new SimpleRelocator("org.apache.thrift7", "org.apache.storm.thrift", oldNamespaceWarnMessage),

                // Following are the shaded packages for which patterns names were changed.
                (Relocator) new SimpleRelocator("org.apache.storm.snakeyaml", "org.apache.storm.shade.org.yaml.snakeyaml", internalClassAccessMessage),
                (Relocator) new SimpleRelocator("org.apache.storm.commons.io", "org.apache.storm.shade.org.apache.commons.io", internalClassAccessMessage),
                (Relocator) new SimpleRelocator("org.apache.storm.commons.codec", "org.apache.storm.shade.org.apache.commons.codec", internalClassAccessMessage),
                (Relocator) new SimpleRelocator("org.apache.storm.commons.lang", "org.apache.storm.shade.org.apache.commons.lang", internalClassAccessMessage),
                (Relocator) new SimpleRelocator("org.apache.storm.json.simple", "org.apache.storm.shade.org.json.simple", internalClassAccessMessage),
                (Relocator) new SimpleRelocator("org.apache.storm.guava.thirdparty", "org.apache.storm.shade.com.google.thirdparty", internalClassAccessMessage),
                (Relocator) new SimpleRelocator("org.apache.storm.guava", "org.apache.storm.shade.com.google.common", internalClassAccessMessage)

        ));
        request.setResourceTransformers(Arrays.asList(
                (ResourceTransformer)new ClojureTransformer()
        ));
        return request;
    }
}
