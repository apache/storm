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
if (LLVM_INCLUDE_DIR)
  set(LLVM_FOUND TRUE)
endif()

if (NOT LLVM_FOUND)
  if (NOT LLVM_CONFIG)
    find_program(LLVM_CONFIG NAMES llvm-config DOC "llvm-config executable")
    if (${LLVM_CONFIG} STREQUAL LLVM_CONFIG-NOTFOUND)
      message(FATAL_ERROR "Cannot find llvm-config")
    endif()
    mark_as_advanced(LLVM_CONFIG)
  endif ()

  execute_process(COMMAND ${LLVM_CONFIG} --cxxflags OUTPUT_VARIABLE LLVM_CXXFLAGS OUTPUT_STRIP_TRAILING_WHITESPACE)

  execute_process(COMMAND ${LLVM_CONFIG} --ldflags --libs core OUTPUT_VARIABLE LLVM_LDFLAGS OUTPUT_STRIP_TRAILING_WHITESPACE)

  set(LLVM_LIBRARIES ${LLVM_LDFLAGS})
  mark_as_advanced(LLVM_LIBRARIES)

  execute_process(COMMAND ${LLVM_CONFIG} --version OUTPUT_VARIABLE LLVM_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
  message(STATUS "Using LLVM ${LLVM_VERSION}")
else()
  if (LLVM_FIND_REQUIRED)
    message(FATAL_ERROR "Could not find LLVM")
  endif()
ENDIF()
