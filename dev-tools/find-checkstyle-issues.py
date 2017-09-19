#!/usr/bin/python
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
import os
from optparse import OptionParser
import subprocess

def getCheckstyleFor(f, check_result):
    f = os.path.abspath(f)
    check_result = os.path.abspath(check_result)
    ret = subprocess.check_output(['xsltproc', '--stringparam', 'target', f, './dev-tools/checkstyle.xslt', check_result])
    if not ret.isspace():
        print ret

def main():
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-c", "--checkstyle-result", dest="check_result",
                      type="string", help="the checkstyle-result.xml file to parse", metavar="FILE")

    (options, args) = parser.parse_args()

    for f in args:
        getCheckstyleFor(f, options.check_result)

if __name__ == "__main__":
    main()
