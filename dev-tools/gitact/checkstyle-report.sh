#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs the build's Checkstyle check on $MODULE and saves its report as
# $RUNNER_TEMP/checkstyle-reports/<label>.xml (and the console output as <label>.txt), plus
# <label>.md: the violation counts and a table of the violations per check, which is also added to
# the job summary. Used by .github/workflows/checkstyle-autofix.yaml.
# The check failing on violations is expected: only a missing report fails this script.

set -u

LABEL=$1
OUT_DIR="$RUNNER_TEMP/checkstyle-reports"
REPORT="$MODULE/target/checkstyle-violation.xml"
mkdir -p "$OUT_DIR"
rm -f "$REPORT"

mvn --batch-mode -pl "$MODULE" org.apache.maven.plugins:maven-checkstyle-plugin:check@validate \
    > "$OUT_DIR/$LABEL.txt" 2>&1

if [[ ! -f "$REPORT" ]]; then
    tail -n 50 "$OUT_DIR/$LABEL.txt"
    echo "::error::Checkstyle did not write $REPORT"
    exit 1
fi
cp "$REPORT" "$OUT_DIR/$LABEL.xml"

python3 - "$REPORT" "$LABEL" > "$OUT_DIR/$LABEL.md" <<'EOF'
import collections
import sys
import xml.etree.ElementTree as ET

report, label = sys.argv[1:]
violations = list(ET.parse(report).getroot().iter('error'))
severities = collections.Counter(v.get('severity') for v in violations)
print(f"### Checkstyle {label} the fix: {severities['error']} errors, {severities['warning']} warnings")
if violations:
    print()
    print('| Check | Severity | Violations |')
    print('|---|---|---|')
    checks = collections.Counter((v.get('source').rsplit('.', 1)[-1].removesuffix('Check'), v.get('severity'))
                                 for v in violations)
    for (check, severity), count in checks.most_common():
        print(f'| {check} | {severity} | {count} |')
print()
EOF
cat "$OUT_DIR/$LABEL.md"
cat "$OUT_DIR/$LABEL.md" >> "$GITHUB_STEP_SUMMARY"
