#!/usr/bin/env python3
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
# Unit tests for dedup-libs.py (same directory). The script name contains a
# hyphen, so it is loaded via importlib rather than a plain import.

import contextlib
import importlib.util
import io
import os
import tempfile
import unittest

_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dedup-libs.py")
_SPEC = importlib.util.spec_from_file_location("dedup_libs", _SCRIPT)
dedup_libs = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(dedup_libs)


class TestDedupLibs(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dist_root = self._tmp.name

    def write_jar(self, directory, name, content):
        path = os.path.join(self.dist_root, directory, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(content)
        return path

    def run_dedup(self):
        """Runs the script's main() and returns (exit_code, stdout, stderr)."""
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = dedup_libs.main(["dedup-libs.py", self.dist_root])
        return code, out.getvalue(), err.getvalue()

    def path(self, *parts):
        return os.path.join(self.dist_root, *parts)

    def test_identical_jars_are_deduped(self):
        self.write_jar("lib", "shared.jar", b"same bytes")
        self.write_jar("lib-worker", "shared.jar", b"same bytes")
        self.write_jar("lib", "daemon-only.jar", b"daemon only")

        code, out, err = self.run_dedup()

        self.assertEqual(code, 0, err)
        # The shared jar lives only in lib-common now; lib-worker is gone.
        self.assertTrue(os.path.exists(self.path("lib-common", "shared.jar")))
        self.assertFalse(os.path.exists(self.path("lib", "shared.jar")))
        self.assertFalse(os.path.exists(self.path("lib-worker")))
        # Daemon-only jars are untouched.
        self.assertTrue(os.path.exists(self.path("lib", "daemon-only.jar")))
        self.assertIn("removed 1 duplicate(s) from lib", out)
        self.assertIn("0 worker-only jar(s)", out)

    def test_diverged_jar_between_lib_and_worker_fails(self):
        self.write_jar("lib", "diverged.jar", b"daemon version")
        self.write_jar("lib-worker", "diverged.jar", b"worker version")
        # An identical shared jar that WOULD be deduped: it must survive a
        # failing run untouched (conflicts are detected before any mutation).
        self.write_jar("lib", "shared.jar", b"same bytes")
        self.write_jar("lib-worker", "shared.jar", b"same bytes")

        code, out, err = self.run_dedup()

        self.assertNotEqual(code, 0)
        self.assertIn("diverged.jar", err)
        # Both sha-256 digests are reported.
        self.assertIn(dedup_libs.sha256(self.path("lib", "diverged.jar")), err)
        self.assertIn(dedup_libs.sha256(self.path("lib-worker", "diverged.jar")), err)
        # The whole tree is untouched: nothing removed from lib, nothing
        # promoted into lib-common, lib-worker still fully populated.
        self.assertTrue(os.path.exists(self.path("lib", "diverged.jar")))
        self.assertTrue(os.path.exists(self.path("lib", "shared.jar")))
        self.assertTrue(os.path.exists(self.path("lib-worker", "diverged.jar")))
        self.assertTrue(os.path.exists(self.path("lib-worker", "shared.jar")))
        self.assertFalse(os.path.exists(self.path("lib-common")))

    def test_diverged_jar_between_lib_and_common_fails(self):
        # Staged-build layout: lib-common is pre-populated, lib-worker absent.
        self.write_jar("lib", "diverged.jar", b"daemon version")
        self.write_jar("lib-common", "diverged.jar", b"worker version")
        self.write_jar("lib", "shared.jar", b"same bytes")
        self.write_jar("lib-common", "shared.jar", b"same bytes")

        code, out, err = self.run_dedup()

        self.assertNotEqual(code, 0)
        self.assertIn("diverged.jar", err)
        self.assertTrue(os.path.exists(self.path("lib", "diverged.jar")))
        self.assertTrue(os.path.exists(self.path("lib-common", "diverged.jar")))
        # The identical shared jar was NOT removed from lib on failure.
        self.assertTrue(os.path.exists(self.path("lib", "shared.jar")))

    def test_diverged_jar_between_worker_and_common_fails(self):
        # A jar already staged in lib-common conflicts with the lib-worker copy.
        self.write_jar("lib-common", "diverged.jar", b"staged version")
        self.write_jar("lib-worker", "diverged.jar", b"worker version")
        self.write_jar("lib", "shared.jar", b"same bytes")
        self.write_jar("lib-worker", "shared.jar", b"same bytes")

        code, out, err = self.run_dedup()

        self.assertNotEqual(code, 0)
        self.assertIn("diverged.jar", err)
        # The conflicting worker copy is kept in place for inspection, and
        # no other jar was moved or removed.
        self.assertTrue(os.path.exists(self.path("lib-worker", "diverged.jar")))
        self.assertTrue(os.path.exists(self.path("lib-common", "diverged.jar")))
        self.assertTrue(os.path.exists(self.path("lib", "shared.jar")))
        self.assertTrue(os.path.exists(self.path("lib-worker", "shared.jar")))
        self.assertFalse(os.path.exists(self.path("lib-common", "shared.jar")))

    def test_all_conflicts_are_reported(self):
        self.write_jar("lib", "first.jar", b"daemon 1")
        self.write_jar("lib-worker", "first.jar", b"worker 1")
        self.write_jar("lib", "second.jar", b"daemon 2")
        self.write_jar("lib-worker", "second.jar", b"worker 2")

        code, out, err = self.run_dedup()

        self.assertNotEqual(code, 0)
        self.assertIn("first.jar", err)
        self.assertIn("second.jar", err)
        self.assertIn("2 conflict(s)", err)
        self.assertIn("no files were changed", err)

    def test_version_drift_between_lib_and_worker_fails(self):
        # A version bump changes the FILE NAME (artifactId-version.jar), so
        # drift must be caught on the version-stripped artifact base name.
        self.write_jar("lib", "commons-io-2.11.0.jar", b"commons-io 2.11.0")
        self.write_jar("lib-worker", "commons-io-2.13.0.jar", b"commons-io 2.13.0")
        self.write_jar("lib", "shared-1.0.jar", b"same bytes")
        self.write_jar("lib-worker", "shared-1.0.jar", b"same bytes")

        code, out, err = self.run_dedup()

        self.assertNotEqual(code, 0)
        self.assertIn("commons-io-2.11.0.jar", err)
        self.assertIn("commons-io-2.13.0.jar", err)
        self.assertIn("different version", err)
        # The whole tree is untouched on failure.
        self.assertTrue(os.path.exists(self.path("lib", "commons-io-2.11.0.jar")))
        self.assertTrue(os.path.exists(self.path("lib", "shared-1.0.jar")))
        self.assertTrue(os.path.exists(self.path("lib-worker", "commons-io-2.13.0.jar")))
        self.assertTrue(os.path.exists(self.path("lib-worker", "shared-1.0.jar")))
        self.assertFalse(os.path.exists(self.path("lib-common")))

    def test_version_drift_between_lib_and_prestaged_common_fails(self):
        # Staged-build layout: lib-common is pre-populated, lib-worker absent.
        self.write_jar("lib", "commons-io-2.11.0.jar", b"commons-io 2.11.0")
        self.write_jar("lib-common", "commons-io-2.13.0.jar", b"commons-io 2.13.0")

        code, out, err = self.run_dedup()

        self.assertNotEqual(code, 0)
        self.assertIn("commons-io-2.11.0.jar", err)
        self.assertIn("commons-io-2.13.0.jar", err)
        self.assertTrue(os.path.exists(self.path("lib", "commons-io-2.11.0.jar")))
        self.assertTrue(os.path.exists(self.path("lib-common", "commons-io-2.13.0.jar")))

    def test_version_drift_between_worker_and_common_fails(self):
        self.write_jar("lib-common", "commons-io-2.11.0.jar", b"commons-io 2.11.0")
        self.write_jar("lib-worker", "commons-io-2.13.0.jar", b"commons-io 2.13.0")

        code, out, err = self.run_dedup()

        self.assertNotEqual(code, 0)
        self.assertIn("commons-io-2.11.0.jar", err)
        self.assertIn("commons-io-2.13.0.jar", err)
        self.assertTrue(os.path.exists(self.path("lib-worker", "commons-io-2.13.0.jar")))
        self.assertTrue(os.path.exists(self.path("lib-common", "commons-io-2.11.0.jar")))

    def test_artifact_base(self):
        for name, base in [
            ("commons-io-2.13.0.jar", "commons-io"),
            ("commons-lang3-3.12.0.jar", "commons-lang3"),
            ("commons-collections4-4.4.jar", "commons-collections4"),
            ("zookeeper-3.9.2.jar", "zookeeper"),
            ("slf4j-api-2.0.13.jar", "slf4j-api"),
            ("kafka-clients-3.7.0.jar", "kafka-clients"),
            ("javax.servlet-api-3.1.0.jar", "javax.servlet-api"),
            ("netty-transport-native-epoll-4.1.100.Final-linux-x86_64.jar",
             "netty-transport-native-epoll"),
            ("storm-shaded-deps.jar", "storm-shaded-deps"),
        ]:
            self.assertEqual(dedup_libs.artifact_base(name), base)

    def test_worker_only_jar_is_promoted_and_counted(self):
        self.write_jar("lib", "shared.jar", b"same bytes")
        self.write_jar("lib-worker", "shared.jar", b"same bytes")
        self.write_jar("lib-worker", "worker-only.jar", b"worker only")

        code, out, err = self.run_dedup()

        self.assertEqual(code, 0, err)
        self.assertTrue(os.path.exists(self.path("lib-common", "worker-only.jar")))
        self.assertIn("1 worker-only jar(s)", out)

    def test_nothing_to_do(self):
        # No lib-worker and no lib-common at all.
        self.write_jar("lib", "daemon-only.jar", b"daemon only")

        code, out, err = self.run_dedup()

        self.assertEqual(code, 0, err)
        self.assertIn("nothing to do", out)
        self.assertTrue(os.path.exists(self.path("lib", "daemon-only.jar")))

    def test_staged_build_layout(self):
        # Staged build: lib-common is already populated, lib-worker is absent.
        self.write_jar("lib", "shared.jar", b"same bytes")
        self.write_jar("lib-common", "shared.jar", b"same bytes")

        code, out, err = self.run_dedup()

        self.assertEqual(code, 0, err)
        self.assertFalse(os.path.exists(self.path("lib", "shared.jar")))
        self.assertTrue(os.path.exists(self.path("lib-common", "shared.jar")))
        self.assertIn("removed 1 duplicate(s) from lib", out)
        self.assertIn("0 worker-only jar(s)", out)

    def test_rerun_is_idempotent(self):
        # Running the script twice (e.g. a re-entrant build) must not fail.
        self.write_jar("lib", "shared.jar", b"same bytes")
        self.write_jar("lib-worker", "shared.jar", b"same bytes")

        code, out, err = self.run_dedup()
        self.assertEqual(code, 0, err)
        code, out, err = self.run_dedup()
        self.assertEqual(code, 0, err)
        # On the second pass the lib copy is already gone, so the shared jar
        # is (correctly) reported as having no counterpart in lib.
        self.assertIn("1 worker-only jar(s)", out)

    def test_usage_error(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = dedup_libs.main(["dedup-libs.py"])
        self.assertEqual(code, 2)
        self.assertIn("Usage", err.getvalue())


if __name__ == "__main__":
    unittest.main()
