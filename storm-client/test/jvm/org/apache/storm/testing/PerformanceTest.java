/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.testing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Tag;

/**
 * Annotation to mark performance tests. Performance tests will be run if the profile performance-tests or all-tests are enabled.
 * <p/>
 * Performance tests can be in the same package as unit tests. To mark a test as a performance test,
 * add the annotation @PerformanceTest to the class definition.
 * For example:
 * <p/>
 * {@literal @}PerformanceTest<br/>
 * public class MyPerformanceTest {<br/>
 *  ...<br/>
 * }
 * <p/>
 *  In general performance tests should have a time limit on them, but the time limit should be liberal enough to account
 *  for running on CI systems like travis ci, or the apache jenkins build.
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Tag("PerformanceTest")
public @interface PerformanceTest {
}
