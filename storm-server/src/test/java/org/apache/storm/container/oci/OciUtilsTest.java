/*
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

package org.apache.storm.container.oci;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.utils.WrappedInvalidTopologyException;
import org.junit.Assert;
import org.junit.Test;

public class OciUtilsTest {

    @Test
    public void validateImageInDaemonConfSkipped() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.STORM_OCI_IMAGE, "storm/rhel7:dev_test");
        //this is essentially a no-op
        OciUtils.validateImageInDaemonConf(conf);
    }

    @Test
    public void validateImageInDaemonConfTest() {
        Map<String, Object> conf = new HashMap<>();
        List<String> allowedImages = new ArrayList<>();
        allowedImages.add("storm/rhel7:dev_test");
        allowedImages.add("storm/rhel7:dev_current");
        conf.put(DaemonConfig.STORM_OCI_ALLOWED_IMAGES, allowedImages);

        conf.put(DaemonConfig.STORM_OCI_IMAGE, "storm/rhel7:dev_test");
        OciUtils.validateImageInDaemonConf(conf);

        allowedImages.add("*");
        conf.put(DaemonConfig.STORM_OCI_IMAGE, "storm/rhel7:wow");
        OciUtils.validateImageInDaemonConf(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateImageInDaemonConfNotInAllowedList() {
        Map<String, Object> conf = new HashMap<>();
        List<String> allowedImages = new ArrayList<>();
        allowedImages.add("storm/rhel7:dev_test");
        conf.put(DaemonConfig.STORM_OCI_ALLOWED_IMAGES, allowedImages);

        conf.put(DaemonConfig.STORM_OCI_IMAGE, "storm/rhel7:wow");
        OciUtils.validateImageInDaemonConf(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateImageInDaemonConfWithNullDefault() {
        Map<String, Object> conf = new HashMap<>();
        List<String> allowedImages = new ArrayList<>();
        allowedImages.add("storm/rhel7:dev_test");
        conf.put(DaemonConfig.STORM_OCI_ALLOWED_IMAGES, allowedImages);

        conf.put(DaemonConfig.STORM_OCI_IMAGE, null); //or not set
        OciUtils.validateImageInDaemonConf(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateImageInDaemonConfWrongPattern() {
        Map<String, Object> conf = new HashMap<>();
        List<String> allowedImages = new ArrayList<>();
        allowedImages.add("*");
        conf.put(DaemonConfig.STORM_OCI_ALLOWED_IMAGES, allowedImages);

        conf.put(DaemonConfig.STORM_OCI_IMAGE, "a-strange@image-name");
        OciUtils.validateImageInDaemonConf(conf);
    }

    @Test
    public void adjustImageConfigForTopoTest() throws InvalidTopologyException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.STORM_OCI_ALLOWED_IMAGES, null); //or not set

        Map<String, Object> topoConf = new HashMap<>();
        String topoId = "topo1";

        //case 1: nothing is not; nothing will happen
        OciUtils.adjustImageConfigForTopo(conf, topoConf, topoId);

        String image1 = "storm/rhel7:dev_test";
        String defaultImage = "storm/rhel7:dev_current";

        //case 2: allowed list is not set; topology oci image will be set to null
        topoConf.put(Config.TOPOLOGY_OCI_IMAGE, image1);
        OciUtils.adjustImageConfigForTopo(conf, topoConf, topoId);
        Assert.assertNull(Config.TOPOLOGY_OCI_IMAGE + " is not removed", topoConf.get(Config.TOPOLOGY_OCI_IMAGE));

        //set up daemon conf properly
        List<String> allowedImages = new ArrayList<>();
        allowedImages.add(image1);
        allowedImages.add(defaultImage);
        conf.put(DaemonConfig.STORM_OCI_ALLOWED_IMAGES, allowedImages);
        conf.put(DaemonConfig.STORM_OCI_IMAGE, defaultImage);

        //case 3: configs are set properly; nothing will happen
        topoConf.put(Config.TOPOLOGY_OCI_IMAGE, image1);
        OciUtils.adjustImageConfigForTopo(conf, topoConf, topoId);
        Assert.assertEquals(image1, topoConf.get(Config.TOPOLOGY_OCI_IMAGE));

        //case 4: topology oci image is not set; will be set to default image
        topoConf.remove(Config.TOPOLOGY_OCI_IMAGE);
        OciUtils.adjustImageConfigForTopo(conf, topoConf, topoId);
        Assert.assertEquals(defaultImage, topoConf.get(Config.TOPOLOGY_OCI_IMAGE));

        //case 5: any topology oci image is allowed
        allowedImages.add("*");
        String image2 = "storm/rhel7:dev_wow";
        topoConf.put(Config.TOPOLOGY_OCI_IMAGE, image2);
        OciUtils.adjustImageConfigForTopo(conf, topoConf, topoId);
        Assert.assertEquals(image2, topoConf.get(Config.TOPOLOGY_OCI_IMAGE));
    }

    @Test(expected = WrappedInvalidTopologyException.class)
    public void adjustImageConfigForTopoNotInAllowedList() throws InvalidTopologyException {
        String image1 = "storm/rhel7:dev_test";
        String image2 = "storm/rhel7:dev_current";

        Map<String, Object> conf = new HashMap<>();
        List<String> allowedImages = new ArrayList<>();
        allowedImages.add(image1);
        conf.put(DaemonConfig.STORM_OCI_ALLOWED_IMAGES, allowedImages);

        Map<String, Object> topoConf = new HashMap<>();
        String topoId = "topo1";
        topoConf.put(Config.TOPOLOGY_OCI_IMAGE, image2);

        OciUtils.adjustImageConfigForTopo(conf, topoConf, topoId);
    }
}