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
# version mismatch is never silently merged. Tool classpaths (lib-tools/*,
# lib-webapp) are intentionally left untouched: their wrappers do not include
# lib-common, so their jars must stay in place.

import hashlib
import os
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


def dedup(dist_root):
    lib = os.path.join(dist_root, "lib")
    worker = os.path.join(dist_root, "lib-worker")
    common = os.path.join(dist_root, "lib-common")

    # Absorb the worker jars into lib-common. This handles two layouts:
    #  - exploded distribution: lib-worker is present, lib-common is created here;
    #  - staged build:          lib-common is already populated, lib-worker is absent.
    worker_jars = jars(worker)
    if worker_jars:
        os.makedirs(common, exist_ok=True)
        for name, src in sorted(worker_jars.items()):
            dst = os.path.join(common, name)
            if not os.path.exists(dst):
                shutil.move(src, dst)
            else:
                os.remove(src)
        # lib-worker is now empty; remove it so the layout is unambiguous.
        if os.path.isdir(worker) and not os.listdir(worker):
            os.rmdir(worker)

    common_jars = jars(common)
    if not common_jars:
        print(f"dedup-libs: no jars in {common} (and none in {worker}); nothing to do")
        return 0

    lib_jars = jars(lib)
    reclaimed = 0
    removed = 0
    for name, common_copy in sorted(common_jars.items()):
        # Drop the byte-identical copy from the daemon lib dir, if present.
        lib_copy = lib_jars.get(name)
        if lib_copy and os.path.exists(lib_copy) and sha256(lib_copy) == sha256(common_copy):
            reclaimed += os.path.getsize(lib_copy)
            os.remove(lib_copy)
            removed += 1

    print(f"dedup-libs: lib-common has {len(common_jars)} jar(s); "
          f"removed {removed} duplicate(s) from lib, reclaimed {reclaimed / (1024 * 1024):.1f} MB")
    return 0


def main(argv):
    if len(argv) != 2:
        print("Usage: dedup-libs.py <dist-root>", file=sys.stderr)
        return 2
    return dedup(argv[1])


if __name__ == "__main__":
    sys.exit(main(sys.argv))
