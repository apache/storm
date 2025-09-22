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


from contextlib import contextmanager
from pathlib import Path
import os
import subprocess
import shlex
import filecmp
import re
import itertools
import argparse

project_root = Path(__file__).resolve().parent.parent
update_dependency_licenses_cmd = ('mvn license:aggregate-add-third-party@generate-and-check-licenses -Dlicense.skipAggregateAddThirdParty=false -B')


@contextmanager
def cd(newdir):
    prevdir = Path.cwd()
    os.chdir(newdir.expanduser())
    try:
        yield
    finally:
        os.chdir(prevdir)


def generate_dependency_licenses():
    """Generates DEPENDENCY-LICENSES in target. The committed DEPENDENCY-LICENSES is not modified."""
    print('Generating DEPENDENCY-LICENSES')
    update_dependency_licenses_output_to_target_cmd = (update_dependency_licenses_cmd +
                                                       ' -Dlicense.thirdPartyFilename=DEPENDENCY-LICENSES' +
                                                       ' -Dlicense.outputDirectory=target')
    subprocess.check_call(shlex.split(
        update_dependency_licenses_output_to_target_cmd))
    print('Done generating DEPENDENCY-LICENSES')


def print_file_contents(msg, file1, file2, show_summary_diff=True, show_file_contents=True):
    """
    Print contents of the files. Used for dumping the actual and expected DEPENDENCY-LICENSES files.
    :param msg: message to print about the files
    :param file1: original file
    :param file2: new file
    :param show_summary_diff: (optional, default True): if true, then print the summary of differences in the files
    :param show_file_contents: (optional, default True): if true, then print the contents of the files
    :return:
    """
    f_names = [file1, file2]
    print('*' * 80)
    print('*' * 30 + msg + ' ' + file1 + ',' + file2 + '*' * 30)

    if show_summary_diff:
        with open(file1, 'r') as f1:
            with open(file2, 'r') as f2:
                diff = set(f1).difference(f2)
        diff.discard('\n')
        print(f'***** Difference between file {file1} and {file2} *******')
        for line in diff:
            print(line)
        print('*' * 80)
    if show_file_contents:
        print('*' * 80)
        for i, f_name in enumerate(f_names):
            print('*' * 30 + ' Start of file ' + f_name + ' ' + '*' * 30)
            print('(' + str(i) + ') File ' + f_name + ' content is:')
            print('\t' + '\t'.join(open(f_name).readlines()))
            print('*' * 30 + ' End of file ' + f_name + ' ' + '*' * 30)
        print('*' * 80)


def check_dependency_licenses():
    """Compares the regenerated DEPENDENCY-LICENSES in target with the DEPENDENCY-LICENSES in the root, and verifies
    that they are identical"""
    print('Checking DEPENDENCY-LICENSES')
    if not filecmp.cmp(Path('DEPENDENCY-LICENSES'), Path('target') / 'DEPENDENCY-LICENSES', shallow=False):
        print(
            f"DEPENDENCY-LICENSES and target/DEPENDENCY-LICENSES are different. "
            f"Please update DEPENDENCY-LICENSES by running '{update_dependency_licenses_cmd}' in the project root")
        print_file_contents('Actual is different from expected', 'DEPENDENCY-LICENSES', 'target/DEPENDENCY-LICENSES')
        return False
    return True


def build_storm():
    print("Building Storm")
    subprocess.check_call(shlex.split(
        'mvn clean install -B -DskipTests -Dcheckstyle.skip -Dpmd.skip'
    ))
    print("Done building Storm")


def extract_license_report_maven_coordinates(lines):
    # Lines like " * Checker Qual (org.checkerframework:checker-qual:2.5.2 - https://checkerframework.org)"
    matches = map(lambda line: re.match(
        r'\s+\*.*\((?P<gav>.*) \- .*\).*', line), lines)
    return set(map(lambda match: match.group('gav'), filter(lambda match: match != None, matches)))


def parse_license_binary_dependencies_coordinate_set():
    """Gets the dependencies listed in LICENSE-binary"""
    license_binary_begin_binary_section = '----------------------------END OF SOURCE NOTICES -------------------------------------------'
    license_binary_lines = read_lines(project_root / 'LICENSE-binary')
    return extract_license_report_maven_coordinates(
        itertools.dropwhile(lambda line: license_binary_begin_binary_section not in line, license_binary_lines))


def extract_dependency_list_maven_coordinates(lines):
    # Lines like "   com.google.code.findbugs:jsr305:jar:3.0.2 -- module jsr305 (auto)"
    matches = map(lambda line: re.match(
        r'\s+(?P<group>\S*)\:(?P<artifact>\S*)\:(?P<type>\S*)\:(?P<version>\S*)', line), lines)
    return set(map(lambda match: match.group('group') + ':' + match.group('artifact') + ':' + match.group('version'), filter(lambda match: match != None, matches)))


def read_lines(path):
    with open(path) as f:
        return f.readlines()


def generate_storm_dist_dependencies_coordinate_set():
    """Gets the dependencies for storm-dist/binary, plus the dependencies of storm-shaded-deps"""
    generated_coordinate_set = extract_license_report_maven_coordinates(read_lines(
        project_root / 'storm-dist' / 'binary' / 'target' / 'generated-sources' / 'license' / 'THIRD-PARTY.txt'))

    # Add dependencies from storm-shaded-deps
    with cd(project_root / 'storm-shaded-deps'):
        print("Generating dependency list for storm-shaded-deps")
        subprocess.check_call(shlex.split(
            'mvn dependency:list -DoutputFile=target/deps-list -Dmdep.outputScope=false -DincludeScope=compile -B'))
        print("Done generating dependency list for storm-shaded-deps")
    shaded_dep_coordinates = extract_dependency_list_maven_coordinates(
        read_lines(project_root / 'storm-shaded-deps' / 'target' / 'deps-list'))
    shaded_dep_coordinates = set(filter(lambda coordinate: 'org.apache.storm:' not in coordinate, shaded_dep_coordinates))
    print('The storm-shaded-deps dependencies that are included when distributing storm-dist/binary are ' + str(shaded_dep_coordinates))
    print('')
    generated_coordinate_set.update(shaded_dep_coordinates)

    return generated_coordinate_set


def generate_storm_dist_license_report():
    with cd(project_root / 'storm-dist' / 'binary'):
        print('')
        print('Generating storm-dist license report')
        subprocess.check_call(shlex.split(update_dependency_licenses_cmd))
        print('Done generating storm-dist license report')


def make_license_binary_checker():
    """
    Checks that the dependencies in the storm-dist/binary license report are mentioned in LICENSE-binary,
    and vice versa.
    """
    print('Checking LICENSE-binary')

    license_binary_coordinate_set = parse_license_binary_dependencies_coordinate_set()
    generated_coordinate_set = generate_storm_dist_dependencies_coordinate_set()
    superfluous_coordinates_in_license = license_binary_coordinate_set.difference(
        generated_coordinate_set)
    coordinates_missing_in_license = generated_coordinate_set.difference(
        license_binary_coordinate_set)
    print('Done checking LICENSE-binary')

    def check_for_errors():
        if superfluous_coordinates_in_license:
            print('Dependencies in LICENSE-binary that appear unused: ')
            for coord in sorted(superfluous_coordinates_in_license):
                print(coord)
        print('')
        if coordinates_missing_in_license:
            print('Dependencies missing from LICENSE-binary: ')
            for coord in sorted(coordinates_missing_in_license):
                print(coord)
        any_wrong_coordinates = coordinates_missing_in_license or superfluous_coordinates_in_license
        if any_wrong_coordinates:
            print('LICENSE-binary needs to be updated. Please remove any unnecessary dependencies from LICENSE-binary, '
                  'and add any that are missing. You can copy any missing dependencies from DEPENDENCY-LICENSES')
        return not any_wrong_coordinates
    return check_for_errors


with cd(project_root):
    parser = argparse.ArgumentParser(description='Validate that the Storm license files are up to date (excluding NOTICE-binary and the licenses/ directory)')
    parser.add_argument('--skip-build-storm', action='store_true', help='set to skip building Storm')
    args = parser.parse_args()
    success = True

    if not args.skip_build_storm:
        build_storm()
    generate_dependency_licenses()
    generate_storm_dist_license_report()
    license_binary_checker = make_license_binary_checker()
    success = check_dependency_licenses() and success
    success = license_binary_checker() and success
    if not success:
        print('Some license files are not up to date, see above for the relevant error message')
        exit(1)
    print('License files are up to date')
    exit(0)
