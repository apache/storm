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

package org.apache.storm.st.meta;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.testng.IExecutionListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.util.Arrays;

/**
 * Testng listener class. This is useful for things that are applicable to all the tests as well
 * taking actions that depend on test results.
 */
public class TestngListener implements ITestListener, IExecutionListener {
    private static final Logger LOGGER = Logger.getLogger(TestngListener.class);
    private final String hr = StringUtils.repeat("-", 100);

    private enum RunResult {SUCCESS, FAILED, SKIPPED, TestFailedButWithinSuccessPercentage }

    @Override
    public void onTestStart(ITestResult result) {
        LOGGER.info(hr);
        LOGGER.info(
                String.format("Testing going to start for: %s.%s(%s)", result.getTestClass().getName(),
                        result.getName(), Arrays.toString(result.getParameters())));
        NDC.push(result.getName());
    }

    private void endOfTestHook(ITestResult result, RunResult outcome) {
        LOGGER.info(
                String.format("Testing going to end for: %s.%s(%s) ----- Status: %s", result.getTestClass().getName(),
                        result.getName(), Arrays.toString(result.getParameters()), outcome));
        NDC.pop();
        LOGGER.info(hr);
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        endOfTestHook(result, RunResult.SUCCESS);
    }

    @Override
    public void onTestFailure(ITestResult result) {
        endOfTestHook(result, RunResult.FAILED);

        LOGGER.info(ExceptionUtils.getStackTrace(result.getThrowable()));
        LOGGER.info(hr);
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        endOfTestHook(result, RunResult.SKIPPED);
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
        endOfTestHook(result, RunResult.TestFailedButWithinSuccessPercentage);
    }

    @Override
    public void onStart(ITestContext context) {
    }

    @Override
    public void onFinish(ITestContext context) {
    }

    @Override
    public void onExecutionStart() {
    }

    @Override
    public void onExecutionFinish() {
    }

}
