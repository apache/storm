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

import argparse
import os
import tempfile
from unittest import TestCase, mock
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

    def test_get_working_dir_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = os.path.join(tmp, "dts-work-dir")
            # pull-build calls this once per image, push-update once more later
            for _ in range(3):
                self.assertEqual(work_dir, dtsq.get_working_dir(work_dir, reuse=True))
            self.assertEqual([], os.listdir(work_dir))

    def test_get_working_dir_default_does_not_use_existing_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            # commands that clean up afterwards must not remove a directory they didn't create
            self.assertEqual(os.path.join(tmp, "docker-to-squash"), dtsq.get_working_dir(tmp))

    def test_pull_build_and_push_update_use_the_same_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = os.path.join(tmp, "dts-work-dir")
            images = ["ubuntu:22.04,ubuntu", "alpine:3.20,alpine", "busybox:1.36,busybox"]
            copied_to = []
            args = argparse.Namespace(skopeo_format="dir", pull_format="docker", images_and_tags=images,
                                      check_magic_file=False, magic_file=None, working_dir=work_dir)

            def fake_copy(pull_format, image, skopeo_format, skopeo_dir):
                os.makedirs(skopeo_dir)
                copied_to.append(skopeo_dir)

            with mock.patch.object(dtsq, "get_manifest_from_docker_image", return_value=({}, "hash")), \
                    mock.patch.object(dtsq, "get_layer_hashes_from_manifest", return_value=[]), \
                    mock.patch.object(dtsq, "get_config_hash_from_manifest", return_value="config"), \
                    mock.patch.object(dtsq, "skopeo_copy_image", side_effect=fake_copy):
                dtsq.pull_build(args)

            self.assertEqual([os.path.join(work_dir, "ubuntu:22.04"), os.path.join(work_dir, "alpine:3.20"),
                              os.path.join(work_dir, "busybox:1.36")], copied_to)

            # push-update must look for the image where pull-build left it
            seen = []
            push_args = argparse.Namespace(image_tag_to_hash="image-tag-to-hash", replication=1, force=False,
                                           images_and_tags=images[:1], bootstrap=False, working_dir=work_dir)

            def fake_local_manifest(path):
                seen.append(path)
                raise RuntimeError("stop after the lookup")

            with mock.patch.object(dtsq, "populate_tag_dicts", return_value=({}, {}, "hash")), \
                    mock.patch.object(dtsq, "get_local_manifest_from_path", side_effect=fake_local_manifest):
                with self.assertRaisesRegex(RuntimeError, "stop after the lookup"):
                    dtsq.push_update(push_args)
            self.assertEqual([os.path.join(work_dir, "ubuntu:22.04", "manifest.json")], seen)

    def test_pull_build_keeps_existing_skopeo_dir_on_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = os.path.join(tmp, "dts-work-dir")
            skopeo_dir = os.path.join(work_dir, "ubuntu:22.04")
            os.makedirs(skopeo_dir)
            leftover = os.path.join(skopeo_dir, "layer.sqsh")
            with open(leftover, "w") as f:
                f.write("previous-run")

            args = argparse.Namespace(skopeo_format="dir", pull_format="docker",
                                      images_and_tags=["ubuntu:22.04,ubuntu"],
                                      check_magic_file=False, magic_file=None, working_dir=work_dir)

            with mock.patch.object(dtsq, "get_manifest_from_docker_image", return_value=({}, "hash")), \
                    mock.patch.object(dtsq, "get_layer_hashes_from_manifest", return_value=[]), \
                    mock.patch.object(dtsq, "get_config_hash_from_manifest", return_value="config"), \
                    mock.patch.object(dtsq, "skopeo_copy_image", side_effect=RuntimeError("copy failed")):
                with self.assertRaisesRegex(RuntimeError, "copy failed"):
                    dtsq.pull_build(args)

            self.assertTrue(os.path.isdir(skopeo_dir))
            self.assertTrue(os.path.isfile(leftover))
            with open(leftover) as f:
                self.assertEqual("previous-run", f.read())

    def test_pull_build_removes_skopeo_dir_created_this_run_on_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = os.path.join(tmp, "dts-work-dir")
            skopeo_dir = os.path.join(work_dir, "ubuntu:22.04")
            args = argparse.Namespace(skopeo_format="dir", pull_format="docker",
                                      images_and_tags=["ubuntu:22.04,ubuntu"],
                                      check_magic_file=False, magic_file=None, working_dir=work_dir)

            def fake_copy(pull_format, image, skopeo_format, dest):
                os.makedirs(dest)
                raise RuntimeError("copy failed")

            with mock.patch.object(dtsq, "get_manifest_from_docker_image", return_value=({}, "hash")), \
                    mock.patch.object(dtsq, "get_layer_hashes_from_manifest", return_value=[]), \
                    mock.patch.object(dtsq, "get_config_hash_from_manifest", return_value="config"), \
                    mock.patch.object(dtsq, "skopeo_copy_image", side_effect=fake_copy):
                with self.assertRaisesRegex(RuntimeError, "copy failed"):
                    dtsq.pull_build(args)

            self.assertFalse(os.path.exists(skopeo_dir))

    # TODO:
    #  def test_read_image_tag_to_hash(self):
    #     """
    #     The base method behaves differently, since string in python3 is always unicode. Base method was flips between
    #     byte arrays and strings. This may not always work properly in python3.
    #     :return:
    #     """
    #     image_tag_to_hash = ""
    #     hash_to_tags, tag_to_hash = dtsq.read_image_tag_to_hash(image_tag_to_hash)

