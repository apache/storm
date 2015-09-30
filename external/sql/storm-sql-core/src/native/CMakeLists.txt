#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_package(JNI REQUIRED)
find_package(LLVM REQUIRED)

# The caller must specify where the generated headers have been placed.
if(NOT GENERATED_JAVAH)
    message(FATAL_ERROR "You must set the CMake variable GENERATED_JAVAH")
endif()

enable_testing()

if(UNIX)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -std=c++11 -g -fPIC -fno-strict-aliasing -fno-exceptions -fno-rtti")
endif()

if(APPLE)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif()

include_directories(
  include
  lib
  third_party/gmock-1.7.0
  third_party/json11)

add_subdirectory(lib)
add_subdirectory(third_party)
add_subdirectory(bindings)
add_subdirectory(test)
