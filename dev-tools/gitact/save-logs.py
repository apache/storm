#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import sys
import subprocess
from datetime import datetime, timedelta


def main(file, cmd):
    print(cmd, "writing to", file)
    out = open(file, "w")
    count = 0
    process = subprocess.Popen(cmd,
                               stderr=subprocess.STDOUT,
                               stdout=subprocess.PIPE)

    start = datetime.now()
    nextPrint = datetime.now() + timedelta(seconds=1)
    # wait for the process to terminate
    pout = process.stdout
    line = pout.readline()
    while line:
        line = line.decode('utf-8')
        count = count + 1
        if datetime.now() > nextPrint:
            diff = datetime.now() - start
            sys.stdout.write(f"\r{diff.seconds} seconds {count} log lines")
            sys.stdout.flush()
            nextPrint = datetime.now() + timedelta(seconds=10)
        out.write(line)
        line = pout.readline()
    out.close()
    errcode = process.wait()
    diff = datetime.now() - start
    sys.stdout.write(f"\r{diff.seconds} seconds {count} log lines")
    print()
    print(cmd, "done", errcode)
    return errcode


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <file-path> <cmd>")
        sys.exit(1)

    sys.exit(main(sys.argv[1], sys.argv[2:]))
