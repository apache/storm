#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Usage: release_notes.py <milestone_id_from_github> > RELEASE_NOTES.html

Depends on "requests", please use pip to install this module.

Generates release notes for a Storm release by generating an HTML doc containing some introductory information about the
 release with links to the Storm docs followed by a list of issues resolved in the release. The script will fail if it finds
 any unresolved issues still marked with the target release. You should run this script after either resolving all issues or
 moving outstanding issues to a later release.

"""

import requests
import sys

if len(sys.argv) < 2:
    print("Usage: release_notes.py <milestone_id>", file=sys.stderr)
    sys.exit(1)

# GitHub configuration
GITHUB_API_BASE_URL = "https://api.github.com"
GITHUB_TOKEN = "YOUR_PERSONAL_GITHUB_PAT"  # Replace with your GitHub PAT

# Input arguments
owner = "apache"
repo = "storm"
milestone = sys.argv[1]  # Milestone ID

print(f"Fetching issues for milestone with id= '{milestone}'...")

headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def get_milestone_title(owner, repo, milestone_number):
    """
    Fetch the title of a specific milestone by its number.
    """
    url = f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/milestones/{milestone_number}"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Failed to fetch milestone: {response.status_code} {response.reason}", file=sys.stderr)
        sys.exit(1)

    milestone = response.json()
    return milestone["title"]

def get_issues(owner, repo, milestone):
    """
    Fetch all issues for a given milestone from a GitHub repository.
    """
    issues_url = f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/issues"
    params = {
        "milestone": milestone,
        "state": "all",  # Include both open and closed issues
        "per_page": 100
    }

    issues = []
    while issues_url:
        response = requests.get(issues_url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Failed to fetch issues: {response.status_code} {response.reason}", file=sys.stderr)
            sys.exit(1)

        data = response.json()
        issues.extend(data)
        # Get next page URL from 'Link' header if available
        issues_url = response.links.get("next", {}).get("url")

    return issues

def issue_link(issue):
    return issue["html_url"]

if __name__ == "__main__":
    issues = get_issues(owner, repo, milestone)

    if not issues:
        print("No issues found for the specified milestone.", file=sys.stderr)
        sys.exit(1)

    unresolved_issues = [issue for issue in issues if issue["state"] != "closed"]
    if unresolved_issues:
        print("The release is not completed since unresolved issues were found:", file=sys.stderr)
        for issue in unresolved_issues:
            print(f"Unresolved issue: {issue['number']:5d} {issue['state']:10s} {issue_link(issue)}", file=sys.stderr)
        sys.exit(1)

    # Group issues by labels
    issues_by_label = {}
    unlabeled_issues = []
    for issue in issues:
        if issue["labels"]:  # If the issue has labels
            for label in issue["labels"]:
                label_name = label["name"]
                issues_by_label.setdefault(label_name, []).append(issue)
        else:
            unlabeled_issues.append(issue)  # Add to the unlabeled list if no labels exist

    # Add unlabeled issues under a special "No Label" category
    if unlabeled_issues:
        issues_by_label["Uncategorized"] = unlabeled_issues

    issues_str = "\n".join([
        f"\n\t<h2>{label}</h2>" +
        f"\n\t<ul>" +
        "\n\t\t".join([
            f'<li>[<a href="{issue_link(issue)}">#{issue["number"]}</a>] - {issue["title"]}</li>'
            for issue in issues
        ]) +
        "\n\t</ul>"
        for label, issues in issues_by_label.items()
    ])

    version = get_milestone_title(owner, repo, milestone)

    print(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Release Notes for Apache Storm {version}</title>
</head>
<body>
<h1>Release Notes for Apache Storm {version}</h1>
<p>Issues addressed in {version}.</p>
{issues_str}
</body>
</html>""")
