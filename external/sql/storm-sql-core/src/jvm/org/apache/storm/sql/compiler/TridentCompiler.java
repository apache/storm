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
package org.apache.storm.sql.compiler;

import java.io.File;
import java.io.IOException;

public class TridentCompiler {
  static {
    System.loadLibrary("stormsql");
  }

  public void compile(File workingDir, String plan) throws IOException {
    String[] s = new String[1];
    int r = compile(plan, workingDir.getAbsolutePath(), s);
    if (r != 0) {
      throw new IOException(s[0]);
    }
  }

  private static native int compile(String plan, String workingDir, String[]
      msg);
}