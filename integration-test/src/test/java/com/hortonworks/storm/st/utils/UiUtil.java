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

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hortonworks.storm.st.utils.TimeUtil;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for UI related tasks.
 */
public final class UiUtil {
    private UiUtil() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final Logger LOGGER = Logger.getLogger(UiUtil.class);

    /**
     * Convert the element to string representation. Useful for debugging/development.
     * @param element element to be converted
     * @param limitDepth the depth to traverse. Typically <=3 is good value.
     * @return
     */
    protected static String elementToString(WebElement element, Integer limitDepth) {
        final StringBuilder retVal =
            new StringBuilder("String representation of the element(first line is format):\n");
        retVal.append("-> tagname")
            .append("(id)")
            .append("(classes)")
            .append("[extra-info]")
            .append("\t")
            .append("text")
            .append("\n");
        retVal.append(elementToString("", element, limitDepth));
        return retVal.toString();
    }

    private static StringBuilder elementToString(String prefix, WebElement element, Integer
        limitDepth) {
        if (limitDepth != null && limitDepth == 0) {
            return new StringBuilder();
        }
        final Integer newDepth = limitDepth == null ? null : limitDepth - 1;
        final StringBuilder elementStr = new StringBuilder(prefix);
        List<String> extraInfo = new ArrayList<>();
        if (StringUtils.isNotBlank(element.getAttribute("ng-repeat"))) {
            extraInfo.add("array");
        }
        elementStr.append("-> ")
            .append(element.getTagName())
            .append("(").append(element.getAttribute("id")).append(")")
            .append("(").append(element.getAttribute("class")).append(")")
            .append(extraInfo)
            .append("\t").append(StringEscapeUtils.escapeJava(element.getText()));
        final String childPrefix = prefix + "\t";
        final List<WebElement> childElements = element.findElements(By.xpath("./*"));
        for (WebElement oneChildElement : childElements) {
            StringBuilder childStr = elementToString(childPrefix, oneChildElement, newDepth);
            if (childStr.length() > 0) {
                elementStr.append("\n").append(childStr);
            }
        }
        return elementStr;
    }

    /**
     * Highlight the element in the UI. Useful for development/debugging.
     * Copied from http://www.testingdiaries.com/highlight-element-using-selenium-webdriver/
     * @param element the element to highlight
     * @param driver the web driver in use
     */
    public static void elementHighlight(WebElement element, WebDriver driver) {
        for (int i = 0; i < 2; i++) {
            JavascriptExecutor js = (JavascriptExecutor) driver;
            js.executeScript(
                "arguments[0].setAttribute('style', arguments[1]);",
                element, "color: red; border: 3px solid red;");
            TimeUtil.sleepSec(2);
            js.executeScript(
                "arguments[0].setAttribute('style', arguments[1]);",
                element, "");
        }
    }
}
