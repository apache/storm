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

"""Usage: update-license-files.py [--skip-build-storm]

Regenerates DEPENDENCY-LICENSES and the binary dependencies section of LICENSE-binary.
Run this after changing dependencies to bring license files up to date.

Depends on "requests" is NOT required. Only needs Maven and Python 3.
"""

from contextlib import contextmanager
from pathlib import Path
import os
import subprocess
import shlex
import shutil
import filecmp
import re
import argparse

project_root = Path(__file__).resolve().parent.parent
update_dependency_licenses_cmd = ('mvn license:aggregate-add-third-party@generate-and-check-licenses -Dlicense.skipAggregateAddThirdParty=false -B')

LICENSE_BINARY_SEPARATOR = '----------------------------END OF SOURCE NOTICES -------------------------------------------'

LICENSE_BINARY_PREAMBLE = """

The following dependencies are included in the binary Storm distributions, in addition to the source dependencies listed above.
The license texts of these dependencies can be found in the licenses directory.
"""


@contextmanager
def cd(newdir):
    prevdir = Path.cwd()
    os.chdir(newdir.expanduser())
    try:
        yield
    finally:
        os.chdir(prevdir)


def read_lines(path):
    with open(path) as f:
        return f.readlines()


def build_storm():
    print("Building Storm")
    subprocess.check_call(shlex.split(
        'mvn clean install -B -DskipTests -Dcheckstyle.skip -Dpmd.skip'
    ))
    print("Done building Storm")


def generate_dependency_licenses():
    """Generates DEPENDENCY-LICENSES in target/. The committed DEPENDENCY-LICENSES is not modified."""
    print('Generating DEPENDENCY-LICENSES')
    cmd = (update_dependency_licenses_cmd +
           ' -Dlicense.thirdPartyFilename=DEPENDENCY-LICENSES' +
           ' -Dlicense.outputDirectory=target')
    subprocess.check_call(shlex.split(cmd))
    print('Done generating DEPENDENCY-LICENSES')


def generate_storm_dist_license_report():
    with cd(project_root / 'storm-dist' / 'binary'):
        print('')
        print('Generating storm-dist license report')
        subprocess.check_call(shlex.split(update_dependency_licenses_cmd))
        print('Done generating storm-dist license report')


def extract_license_report_maven_coordinates(lines):
    """Extract Maven coordinates from license report lines.
    Lines like: ' * Checker Qual (org.checkerframework:checker-qual:2.5.2 - https://checkerframework.org)'
    """
    matches = map(lambda line: re.match(
        r'\s+\*.*\((?P<gav>.*) \- .*\).*', line), lines)
    return set(map(lambda match: match.group('gav'), filter(lambda match: match is not None, matches)))


def extract_dependency_list_maven_coordinates(lines):
    """Extract Maven coordinates from 'mvn dependency:list' output.
    Lines like: '   com.google.code.findbugs:jsr305:jar:3.0.2 -- module jsr305 (auto)'
    """
    matches = map(lambda line: re.match(
        r'\s+(?P<group>\S*)\:(?P<artifact>\S*)\:(?P<type>\S*)\:(?P<version>\S*)', line), lines)
    return set(map(lambda match: match.group('group') + ':' + match.group('artifact') + ':' + match.group('version'),
                   filter(lambda match: match is not None, matches)))


def get_shaded_dep_coordinates():
    """Gets the set of Maven coordinates for storm-shaded-deps (excluding Storm's own modules)."""
    with cd(project_root / 'storm-shaded-deps'):
        print("Generating dependency list for storm-shaded-deps")
        subprocess.check_call(shlex.split(
            'mvn dependency:list -DoutputFile=target/deps-list -Dmdep.outputScope=false -DincludeScope=compile -B'))
        print("Done generating dependency list for storm-shaded-deps")
    shaded_dep_coordinates = extract_dependency_list_maven_coordinates(
        read_lines(project_root / 'storm-shaded-deps' / 'target' / 'deps-list'))
    shaded_dep_coordinates = set(filter(lambda coordinate: 'org.apache.storm:' not in coordinate, shaded_dep_coordinates))
    print('storm-shaded-deps dependencies: ' + str(shaded_dep_coordinates))
    print('')
    return shaded_dep_coordinates


def parse_grouped_license_file(lines):
    """Parse a license report file (DEPENDENCY-LICENSES format) into structured groups.

    Returns a list of (header_line, [entry_lines]) tuples, preserving blank lines and formatting.
    The header_line is the license group name (e.g. '    Apache License, Version 2.0\\n').
    entry_lines are the dependency lines under that group (e.g. '        * Foo (g:a:v - url)\\n').
    """
    groups = []
    current_header = None
    current_entries = []

    for line in lines:
        # Skip the file header (first few lines before any license group)
        stripped = line.rstrip('\n')

        # License group headers are indented with 4 spaces and have non-whitespace content
        if re.match(r'    \S', line) and not line.strip().startswith('*'):
            if current_header is not None:
                groups.append((current_header, current_entries))
            current_header = line
            current_entries = []
        elif current_header is not None:
            current_entries.append(line)

    if current_header is not None:
        groups.append((current_header, current_entries))

    return groups


def filter_groups_to_coordinates(groups, target_coordinates):
    """Filter license groups to only include entries whose coordinates are in the target set.

    Returns lines for the filtered license report section.
    """
    result_lines = []
    gav_pattern = re.compile(r'\s+\*.*\((?P<gav>.*) \- .*\).*')

    for header, entries in groups:
        filtered_entries = []
        for entry in entries:
            match = gav_pattern.match(entry)
            if match:
                if match.group('gav') in target_coordinates:
                    filtered_entries.append(entry)
            # Keep blank lines between entries only if we have entries
        if filtered_entries:
            result_lines.append('\n')
            result_lines.append(header)
            result_lines.append('\n')
            for entry in filtered_entries:
                result_lines.append(entry)

    return result_lines


def update_dependency_licenses():
    """Copy target/DEPENDENCY-LICENSES to root DEPENDENCY-LICENSES.
    Returns True if the file changed."""
    src = project_root / 'target' / 'DEPENDENCY-LICENSES'
    dst = project_root / 'DEPENDENCY-LICENSES'
    if dst.exists() and filecmp.cmp(src, dst, shallow=False):
        print('DEPENDENCY-LICENSES is already up to date')
        return False
    shutil.copy2(src, dst)
    print('Updated DEPENDENCY-LICENSES')
    return True


def merge_groups(base_groups, extra_groups, extra_coordinates):
    """Merge extra_groups entries into base_groups for coordinates in extra_coordinates
    that are not already present in base_groups.

    Returns a new list of (header, [entry_lines]) tuples.
    """
    # Collect all coordinates already in base_groups
    gav_pattern = re.compile(r'\s+\*.*\((?P<gav>.*) \- .*\).*')
    base_coords = set()
    for _, entries in base_groups:
        for entry in entries:
            match = gav_pattern.match(entry)
            if match:
                base_coords.add(match.group('gav'))

    # Find extra entries to add (in extra_coordinates but not in base)
    missing_coords = extra_coordinates - base_coords
    if not missing_coords:
        return base_groups

    print(f'Adding {len(missing_coords)} shaded-deps entries from DEPENDENCY-LICENSES to LICENSE-binary')

    # Build a map: header_text -> list of entry lines to add
    extra_by_header = {}
    for header, entries in extra_groups:
        header_key = header.strip()
        for entry in entries:
            match = gav_pattern.match(entry)
            if match and match.group('gav') in missing_coords:
                extra_by_header.setdefault(header_key, []).append(entry)

    # Merge into base_groups
    result = []
    seen_headers = set()
    for header, entries in base_groups:
        header_key = header.strip()
        seen_headers.add(header_key)
        merged_entries = list(entries)
        if header_key in extra_by_header:
            merged_entries.extend(extra_by_header[header_key])
        result.append((header, merged_entries))

    # Add any license groups that only exist in extra
    for header_key, extra_entries in extra_by_header.items():
        if header_key not in seen_headers:
            result.append(('    ' + header_key + '\n', extra_entries))

    return result


def groups_to_lines(groups):
    """Convert parsed groups back to lines for writing."""
    result_lines = []
    for header, entries in sorted(groups, key=lambda g: g[0].strip().lower()):
        result_lines.append('\n')
        result_lines.append(header)
        result_lines.append('\n')
        for entry in entries:
            # Skip blank lines that were part of the original inter-group spacing
            if entry.strip():
                result_lines.append(entry)
    return result_lines


def update_license_binary(shaded_dep_coordinates):
    """Replace the binary dependencies section of LICENSE-binary.

    Uses storm-dist/binary THIRD-PARTY.txt as the base (already has all binary deps grouped),
    then merges in any storm-shaded-deps entries from DEPENDENCY-LICENSES that aren't already
    covered by the binary THIRD-PARTY.txt.

    Returns True if the file changed."""
    license_binary_path = project_root / 'LICENSE-binary'
    lines = read_lines(license_binary_path)

    # Find the separator line
    separator_idx = None
    for i, line in enumerate(lines):
        if LICENSE_BINARY_SEPARATOR in line:
            separator_idx = i
            break

    if separator_idx is None:
        print(f'ERROR: Could not find separator line in LICENSE-binary: "{LICENSE_BINARY_SEPARATOR}"')
        return False

    # Keep the static header (up to and including the separator)
    static_part = lines[:separator_idx + 1]

    # Use the binary THIRD-PARTY.txt as the base — it already contains all
    # storm-dist/binary dependencies in the correct grouped format
    binary_third_party_path = (project_root / 'storm-dist' / 'binary' / 'target' /
                               'generated-sources' / 'license' / 'THIRD-PARTY.txt')
    binary_groups = parse_grouped_license_file(read_lines(binary_third_party_path))

    # Merge in storm-shaded-deps entries from DEPENDENCY-LICENSES
    dep_licenses_lines = read_lines(project_root / 'target' / 'DEPENDENCY-LICENSES')
    dep_licenses_groups = parse_grouped_license_file(dep_licenses_lines)
    merged_groups = merge_groups(binary_groups, dep_licenses_groups, shaded_dep_coordinates)

    binary_section = groups_to_lines(merged_groups)

    # Compose the new file
    preamble_lines = LICENSE_BINARY_PREAMBLE.splitlines(keepends=True)
    new_content = static_part + preamble_lines + binary_section

    # Compare with current content
    if new_content == lines:
        print('LICENSE-binary is already up to date')
        return False

    with open(license_binary_path, 'w') as f:
        f.writelines(new_content)
    print('Updated LICENSE-binary')
    return True


if __name__ == '__main__':
    with cd(project_root):
        parser = argparse.ArgumentParser(
            description='Update Storm license files (DEPENDENCY-LICENSES and LICENSE-binary)')
        parser.add_argument('--skip-build-storm', action='store_true',
                            help='skip building Storm (use if already built)')
        args = parser.parse_args()

        try:
            if not args.skip_build_storm:
                build_storm()
            generate_dependency_licenses()
            generate_storm_dist_license_report()

            shaded_dep_coordinates = get_shaded_dep_coordinates()

            dep_changed = update_dependency_licenses()
            lic_changed = update_license_binary(shaded_dep_coordinates)

            if dep_changed or lic_changed:
                print('\nLicense files were updated. Please review the changes.')
            else:
                print('\nLicense files are already up to date. No changes made.')
        except subprocess.CalledProcessError as e:
            print(f'Command failed: {e}', flush=True)
            exit(1)
        except Exception as e:
            print(f'Error updating license files: {e}', flush=True)
            exit(1)
