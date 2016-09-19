/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.storm.st.utils;

import org.hortonworks.storm.st.utils.TimeUtil;
import org.openqa.selenium.WebElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

/**
 * Created by Raghav Kumar Gautam on 7/6/16.
 */

public final class UiAssert {
    private UiAssert() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(UiAssert.class);

    public static void assertDisplayed(WebElement element, String webElementName) {
        LOGGER.info(String.format("Checking if WebElement '%s' is displayed", webElementName));
        int timeoutSeconds = 5;
        for (int i = 0; !element.isDisplayed() && i < timeoutSeconds * 10; i++) {
            TimeUtil.sleepMilliSec(100);
        }
        Assert.assertTrue(element.isDisplayed(),
                String.format("WebElement '%s' should have been displayed", webElementName));
        LOGGER.info(String.format("WebElement '%s' is displayed", webElementName));
    }

    public static void assertNotDisplayed(WebElement clusterForm, String webElementName) {
        LOGGER.info(String.format("Checking if WebElement '%s' is displayed", webElementName));
        Assert.assertFalse(clusterForm.isDisplayed(),
                String.format("WebElement '%s' should NOT have been displayed", webElementName));
        LOGGER.info(String.format("WebElement '%s' is not displayed", webElementName));
    }
}
