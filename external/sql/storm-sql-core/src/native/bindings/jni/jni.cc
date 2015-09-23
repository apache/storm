/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "stormsql/stormsql.h"
#include "org_apache_storm_sql_compiler_TridentCompiler.h"

using namespace stormsql;

#undef JNIEXPORT
#define JNIEXPORT __attribute__ ((visibility ("default")))

static inline std::string GetString(JNIEnv *env, jstring str) {
  const char *ptr = env->GetStringUTFChars(str, nullptr);
  jsize length = env->GetStringUTFLength(str);
  std::string plan(ptr, length);
  env->ReleaseStringUTFChars(str, ptr);
  return plan;
}

JNIEXPORT jint JNI_OnLoad(JavaVM* vm, void*)
{
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6) != JNI_OK) {
    return -1;
  }

  return JNI_VERSION_1_6;
}

JNIEXPORT jint JNICALL
Java_org_apache_storm_sql_compiler_TridentCompiler_compile(JNIEnv *env, jclass,
                                                           jstring plan,
                                                           jstring working_dir,
                                                           jobjectArray err) {
  std::string msg;
  int ret = CompilePlanToTrident(GetString(env, plan),
                                 GetString(env, working_dir), &msg);
  if (ret) {
    jstring s = env->NewStringUTF(msg.c_str());
    env->SetObjectArrayElement(err, 0, s);
  }
  return ret;
}

