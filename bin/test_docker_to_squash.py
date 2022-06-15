#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import TestCase
# import docker-to-squash as dtsq
dtsq = __import__('docker-to-squash')  # TODO: rename docker-to-squash.py as docker_to_squash.py


class Test(TestCase):
    def test_shell_command(self):
        """
        shell_command is used by many functions in docker_to_squash.py. Ensure that it works correctly.
        Prior code was not returning any values, and was not detected till PR https://github.com/apache/storm/pull/3475
        :return:
        """
        # expect success
        cmd = ["ls", "-l"]
        out, err, rc = dtsq.shell_command(cmd, True, True, True, timeout_sec=10)
        self.assertEqual(0, rc, f"Failed cmd={cmd}\nrc={rc}\nout={out}\nerr={err}")

        # expect failure
        cmd = ["badcmd", "-l"]
        out, err, rc = dtsq.shell_command(cmd, True, True, True, timeout_sec=10)
        self.assertNotEqual(0, rc, f"Expected to fail cmd={cmd}\nrc={rc}\nout={out}\nerr={err}")

    # TODO:
    #  def test_read_image_tag_to_hash(self):
    #     """
    #     The base method behaves differently, since string in python3 is always unicode. Base method was flips between
    #     byte arrays and strings. This may not always work properly in python3.
    #     :return:
    #     """
    #     image_tag_to_hash = ""
    #     hash_to_tags, tag_to_hash = dtsq.read_image_tag_to_hash(image_tag_to_hash)

