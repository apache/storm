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
# De-duplicates the jars shared by the daemon (lib) and worker (lib-worker)
# classpaths of an assembled Storm distribution into a single lib-common
# directory, to keep the distribution small.
#
# The worker classpath is a strict subset of the daemon classpath, so every
# lib-worker jar is moved into lib-common; any byte-identical copy in lib is
# then removed. bin/storm.py adds lib-common to BOTH classpaths, so:
#   daemon classpath = lib-common + lib       (unchanged jar set)
#   worker classpath = lib-common (+ lib-worker, now empty)
#
# Only byte-identical jars (same name AND same sha-256) are de-duplicated, so a
# version mismatch is never silently merged. The script fails the build when
# the two classpaths carry conflicting copies of an artifact, in either form:
#  - same file name, different sha-256 (e.g. a patched rebuild), or
#  - same Maven artifact, different version. The jars are named
#    <artifactId>-<version>[-<classifier>].jar by maven-dependency-plugin, so a
#    version bump changes the FILE NAME and a plain name comparison would let
#    e.g. lib/foo-1.0.jar and lib-worker/foo-1.1.jar pass silently. Jars are
#    therefore compared by their version-stripped artifact base name (the part
#    before the first "-<digit>", the same heuristic Maven uses to start the
#    version).
# Either way both copies would land on the daemon classpath, with the
# lib-common copy silently shadowing the lib one. All conflicts are detected in
# a read-only pass BEFORE any file is moved or removed, so a failing run leaves
# the distribution tree exactly as it found it. Tool classpaths (lib-tools/*,
# lib-webapp) are intentionally left untouched: their wrappers do not include
# lib-common, so their jars must stay in place.
#
# The summary also reports the number of worker-only jars (jars promoted to
# lib-common whose artifact does not appear in lib at all). The worker
# classpath is a strict subset of the daemon classpath in a single-reactor
# build, so this count is expected to be 0; a non-zero value is an early
# warning that the two classpaths have drifted apart.

import hashlib
import os
import re
import shutil
import sys


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def jars(directory):
    if not os.path.isdir(directory):
        return {}
    return {f: os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".jar")}


def artifact_base(name):
    """Version-stripped artifact base of a maven-built jar file name.

    "commons-io-2.13.0.jar" -> "commons-io"; the version starts at the first
    "-" that is followed by a digit (Maven artifactId segments do not start
    with a digit). A name without such a separator is returned as-is (minus
    the .jar suffix)."""
    stem = name[: -len(".jar")]
    match = re.match(r"(.+?)-\d", stem)
    return match.group(1) if match else stem


def by_base(jar_names):
    bases = {}
    for name in jar_names:
        bases.setdefault(artifact_base(name), []).append(name)
    return bases


def dedup(dist_root):
    lib = os.path.join(dist_root, "lib")
    worker = os.path.join(dist_root, "lib-worker")
    common = os.path.join(dist_root, "lib-common")

    # Two supported layouts:
    #  - exploded distribution: lib-worker is present, lib-common is created here;
    #  - staged build:          lib-common is already populated, lib-worker is absent.
    worker_jars = jars(worker)
    common_jars = jars(common)
    lib_jars = jars(lib)

    if not worker_jars and not common_jars:
        print(f"dedup-libs: no jars in {common} (and none in {worker}); nothing to do")
        return 0

    # ---- Phase 1: read-only conflict detection. Hash each jar (at most once)
    # and collect every same-name/different-sha pair BEFORE anything is moved
    # or removed, so a failing run leaves the tree exactly as it found it.
    hashes = {}

    def digest(path):
        if path not in hashes:
            hashes[path] = sha256(path)
        return hashes[path]

    # Conflicting jars between two classpath dirs: same file name with a
    # different sha-256, or same artifact base with a different version (which
    # a name comparison alone would miss, since the version is part of the
    # file name). Each entry is a fully formatted message naming both copies.
    conflicts = []
    common_bases = by_base(common_jars)
    for name in sorted(worker_jars):
        if name in common_jars:
            if digest(worker_jars[name]) != digest(common_jars[name]):
                conflicts.append(f"{name}: lib-worker sha-256 {digest(worker_jars[name])} "
                                 f"!= lib-common sha-256 {digest(common_jars[name])}")
        else:
            for other in sorted(common_bases.get(artifact_base(name), [])):
                conflicts.append(f"{name}: lib-worker has {name} but lib-common has {other} "
                                 f"(artifact '{artifact_base(name)}', different version)")

    # The jar set lib-common will hold after absorbing lib-worker. On a
    # worker-vs-common conflict the winner is ambiguous, but we fail anyway.
    merged = dict(common_jars)
    for name, path in worker_jars.items():
        merged.setdefault(name, path)

    lib_bases = by_base(lib_jars)
    lib_duplicates = []
    worker_only = 0
    for name, merged_copy in sorted(merged.items()):
        origin = "lib-common" if name in common_jars else "lib-worker"
        lib_copy = lib_jars.get(name)
        if lib_copy:
            if digest(lib_copy) == digest(merged_copy):
                # Byte-identical copy in the daemon lib dir; removable in phase 2.
                lib_duplicates.append(lib_copy)
            else:
                # Same name, different bytes: the lib-common copy would silently
                # shadow the lib copy on the daemon classpath. Fail the build.
                conflicts.append(f"{name}: lib sha-256 {digest(lib_copy)} "
                                 f"!= {origin} sha-256 {digest(merged_copy)}")
        else:
            siblings = sorted(n for n in lib_bases.get(artifact_base(name), []))
            if siblings:
                # Same artifact, different version: promoting this jar would put
                # both versions on the daemon classpath. Fail the build.
                conflicts.append(f"{name}: {origin} has {name} but lib has {', '.join(siblings)} "
                                 f"(artifact '{artifact_base(name)}', different version)")
            else:
                # Artifact absent from lib entirely: worker-only. Expected to
                # never happen in a single-reactor build; reported as a canary.
                worker_only += 1

    if conflicts:
        for conflict in conflicts:
            print(f"dedup-libs: ERROR: conflicting jar {conflict}", file=sys.stderr)
        print(f"dedup-libs: ERROR: {len(conflicts)} conflict(s) (same name with different content, or same "
              "artifact with a different version) between the daemon (lib) and worker "
              "(lib-common/lib-worker) classpaths; "
              "refusing to assemble a distribution with shadowed jars; no files were changed", file=sys.stderr)
        return 1

    # ---- Phase 2: mutation. No conflicts exist, so every same-name pair
    # encountered below is known to be byte-identical.
    if worker_jars:
        os.makedirs(common, exist_ok=True)
        for name, src in sorted(worker_jars.items()):
            dst = os.path.join(common, name)
            if os.path.exists(dst):
                os.remove(src)
            else:
                shutil.move(src, dst)
        # lib-worker is now empty; remove it so the layout is unambiguous.
        if os.path.isdir(worker) and not os.listdir(worker):
            os.rmdir(worker)

    # Drop the byte-identical copies from the daemon lib dir.
    reclaimed = 0
    for lib_copy in lib_duplicates:
        reclaimed += os.path.getsize(lib_copy)
        os.remove(lib_copy)

    print(f"dedup-libs: lib-common has {len(merged)} jar(s); "
          f"removed {len(lib_duplicates)} duplicate(s) from lib, reclaimed {reclaimed / (1024 * 1024):.1f} MB; "
          f"{worker_only} worker-only jar(s) promoted to lib-common (expected 0)")
    return 0


def main(argv):
    if len(argv) != 2:
        print("Usage: dedup-libs.py <dist-root>", file=sys.stderr)
        return 2
    return dedup(argv[1])


if __name__ == "__main__":
    sys.exit(main(sys.argv))
