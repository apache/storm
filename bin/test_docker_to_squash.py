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

import hashlib
import json
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

        # expect failure, without raise_on_error the return code is returned
        for cmd in (["badcmd", "-l"], ["false"]):
            out, err, rc = dtsq.shell_command(cmd, True, True, False, timeout_sec=10)
            self.assertNotEqual(0, rc, f"Expected to fail cmd={cmd}\nrc={rc}\nout={out}\nerr={err}")

        # expect failure, with raise_on_error an exception is raised
        for cmd in (["badcmd", "-l"], ["false"]):
            with self.assertRaises(Exception, msg=f"Expected to raise cmd={cmd}"):
                dtsq.shell_command(cmd, True, True, True, timeout_sec=10)

    def test_get_image_without_tag_or_digest(self):
        cases = {
            "ubuntu": "ubuntu",
            "ubuntu:22.04": "ubuntu",
            "myorg/app:1": "myorg/app",
            "registry.example.com/team/app:1.0": "registry.example.com/team/app",
            "localhost:5000/app": "localhost:5000/app",
            "localhost:5000/team/app:1.0": "localhost:5000/team/app",
            "myorg/app@sha256:abc": "myorg/app",
            "myorg/app:1@sha256:abc": "myorg/app",
        }
        for image, expected in cases.items():
            self.assertEqual(expected, dtsq.get_image_without_tag_or_digest(image), image)

    def test_get_manifest_from_docker_image_keeps_repository_for_manifest_list(self):
        manifest_list = json.dumps({"manifests": [
            {"digest": "sha256:arm", "platform": {"architecture": "arm64"}},
            {"digest": "sha256:amd", "platform": {"architecture": "amd64"}},
        ]}).encode()
        image_manifest = json.dumps({"layers": []}).encode()
        inspected = []

        def fake_shell_command(cmd, *args, **kwargs):
            inspected.append(cmd[-1])
            return (manifest_list if len(inspected) == 1 else image_manifest), b"", 0

        # calculate_string_hash is stubbed: it is handed a str today and can't hash it on Python 3,
        # which is a separate problem from the one tested here.
        with mock.patch.object(dtsq, "shell_command", side_effect=fake_shell_command), \
                mock.patch.object(dtsq, "calculate_string_hash", return_value="hash"):
            dtsq.get_manifest_from_docker_image("docker", "registry.example.com/team/app:1.0")

        self.assertEqual(["docker://registry.example.com/team/app:1.0",
                          "docker://registry.example.com/team/app@sha256:amd"], inspected)

    def test_calculate_bytes_hash_matches_hashlib(self):
        """
        Manifest hashes must be computed over the raw manifest bytes, not over
        str(bytes) (which hashes the literal "b'...'" repr and produces wrong hashes).
        """
        manifest = b'{"schemaVersion":2,"config":{"digest":"sha256:abc"}}'
        expected = hashlib.sha256(manifest).hexdigest()
        self.assertEqual(expected, dtsq.calculate_bytes_hash(manifest))
        # The old code hashed str(manifest); that must NOT equal the real hash.
        self.assertNotEqual(expected, dtsq.calculate_string_hash(str(manifest)))
        # The str helper still agrees with the bytes helper for real text.
        self.assertEqual(expected, dtsq.calculate_string_hash(manifest.decode("utf-8")))

    def test_decode_output(self):
        self.assertEqual("root:root", dtsq.decode_output(b"root:root\n").strip())
        self.assertEqual("", dtsq.decode_output(None))
        self.assertEqual("already text", dtsq.decode_output("already text"))

    def test_read_write_image_tag_to_hash_round_trip(self):
        """
        The tag file written by write_local_image_tag_to_hash must read back
        identically. Before the fix, read_image_tag_to_hash did str(line) on
        bytes, mangling every entry (e.g. "b'img:hash\\n'") so the hash failed
        validation and the entry was silently skipped.
        """
        manifest_hash = hashlib.sha256(b"fake-manifest").hexdigest()
        other_hash = hashlib.sha256(b"other-manifest").hexdigest()
        hash_to_tags = {
            manifest_hash: (["img:latest", "img:1.0"], ["first upload"]),
            other_hash: (["img:2.0"], []),
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            tag_file = os.path.join(tmp_dir, "image-tag-to-hash")
            dtsq.write_local_image_tag_to_hash(tag_file, hash_to_tags)
            read_hash_to_tags, read_tag_to_hash = dtsq.read_image_tag_to_hash(tag_file)
        self.assertEqual(sorted(["img:1.0", "img:latest"]),
                         sorted(read_hash_to_tags[manifest_hash][0]))
        self.assertEqual(["first upload"], read_hash_to_tags[manifest_hash][1])
        self.assertEqual(["img:2.0"], read_hash_to_tags[other_hash][0])
        self.assertEqual(manifest_hash, read_tag_to_hash["img:latest"])
        self.assertEqual(manifest_hash, read_tag_to_hash["img:1.0"])
        self.assertEqual(other_hash, read_tag_to_hash["img:2.0"])

