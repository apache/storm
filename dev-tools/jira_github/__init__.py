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

import re
import urllib
import urllib.request
import urllib.parse
from datetime import datetime

try:
    import json
except ImportError:
    import simplejson as json


def jiratime(obj):
    if obj is None:
        return None
    return datetime.strptime(obj[0:19], "%Y-%m-%dT%H:%M:%S")


# Regex pattern definitions
github_user = re.compile(r"Git[Hh]ub user ([\w-]+)")
github_pull = re.compile(r"https://github.com/[^/\s]+/[^/\s]+/pull/[0-9]+")
has_vote = re.compile(r"\s+([-+][01])\s*")
is_diff = re.compile("--- End diff --")


def search_group(reg, txt, group):
    m = reg.search(txt)
    if m is None:
        return None
    return m.group(group)


class JiraComment:
    """A comment on a JIRA"""

    def __init__(self, data):
        self.data = data
        self.author = self.data['author']['name']
        self.github_author = None
        self.githubPull = None
        self.githubComment = (self.author == "githubbot")
        body = self.get_body()
        if is_diff.search(body) is not None:
            self.vote = None
        else:
            self.vote = search_group(has_vote, body, 1)

        if self.githubComment:
            self.github_author = search_group(github_user, body, 1)
            self.githubPull = search_group(github_pull, body, 0)

    def get_author(self):
        if self.github_author is not None:
            return self.github_author
        return self.author

    def get_body(self):
        return self.data['body']

    def get_pull(self):
        return self.githubPull

    def has_github_pull(self):
        return self.githubPull is not None

    def raw(self):
        return self.data

    def has_vote(self):
        return self.vote is not None

    def get_vote(self):
        return self.vote

    def get_created(self):
        return jiratime(self.data['created'])


class Jira:
    """A single JIRA"""

    def __init__(self, data, parent):
        self.key = data['key']
        self.fields = data['fields']
        self.parent = parent
        self.notes = None
        self.comments = None

    def get_id(self):
        """
        Get Jira ID as a string from the string stored in self.key
        :return: Jira id, example "STORM-1234"
        """
        return self.key

    def get_id_num(self):
        """
        Get Jira ID number as an integer from the string stored in self.key
        :return: Numeric Jira Id as a number. Example "STORM-1234" and "ZKP-1234" will both return 1234
        """
        return int(self.key.split('-')[-1])

    def get_description(self):
        return self.fields['description']

    def getReleaseNote(self):
        if self.notes is None:
            field = self.parent.fieldIdMap['Release Note']
            if field in self.fields:
                self.notes = self.fields[field]
            else:
                self.notes = self.get_description()
        return self.notes

    def get_status(self):
        ret = ""
        status = self.fields['status']
        if status is not None:
            ret = status['name']
        return ret

    def get_priority(self):
        ret = ""
        pri = self.fields['priority']
        if pri is not None:
            ret = pri['name']
        return ret

    def get_assignee_email(self):
        ret = ""
        mid = self.fields['assignee']
        if mid is not None:
            ret = mid['emailAddress']
        return ret

    def get_assignee(self):
        ret = ""
        mid = self.fields['assignee']
        if mid is not None:
            ret = mid['displayName']
        return ret

    def get_components(self):
        return " , ".join([comp['name'] for comp in self.fields['components']])

    def get_summary(self):
        return self.fields['summary']

    def get_trimmed_summary(self):
        limit = 40
        summary = self.fields['summary']
        return summary if len(summary) < limit else summary[0:limit] + "..."

    def get_type(self):
        ret = ""
        mid = self.fields['issuetype']
        if mid is not None:
            ret = mid['name']
        return ret

    def get_reporter(self):
        ret = ""
        mid = self.fields['reporter']
        if mid is not None:
            ret = mid['displayName']
        return ret

    def get_project(self):
        ret = ""
        mid = self.fields['project']
        if mid is not None:
            ret = mid['key']
        return ret

    def get_created(self):
        return jiratime(self.fields['created'])

    def get_updated(self):
        return jiratime(self.fields['updated'])

    def get_comments(self):
        if self.comments is None:
            jiraId = self.get_id()
            comments = []
            at = 0
            end = 1
            count = 100
            while at < end:
                params = urllib.parse.urlencode({'startAt': at, 'maxResults': count})
                resp = urllib.request.urlopen(self.parent.baseUrl + "/issue/" + jiraId + "/comment?" + params)
                resp_str = resp.read().decode()
                data = json.loads(resp_str)
                if 'errorMessages' in data:
                    raise Exception(data['errorMessages'])
                at = data['startAt'] + data['maxResults']
                end = data['total']
                for item in data['comments']:
                    j = JiraComment(item)
                    comments.append(j)
            self.comments = comments
        return self.comments

    def has_voted_comment(self):
        for comment in self.get_comments():
            if comment.has_vote():
                return True
        return False

    def get_trimmed_comments(self, limit=40):
        comments = self.get_comments()
        return comments if len(comments) < limit else comments[0:limit] + ["..."]

    def raw(self):
        return self.fields

    def storm_jira_cmp(self, x, y):
        xn = x.get_id().split("-")[1]
        yn = y.get_id().split("-")[1]
        return int(xn) - int(yn)


class JiraRepo:
    """A Repository for JIRAs"""

    def __init__(self, baseUrl):
        self.baseUrl = baseUrl
        resp = urllib.request.urlopen(baseUrl + "/field")
        resp_str = resp.read().decode()
        data = json.loads(resp_str)

        self.fieldIdMap = {}
        for part in data:
            self.fieldIdMap[part['name']] = part['id']

    def get(self, id):
        resp = urllib.request.urlopen(self.baseUrl + "/issue/" + id)
        resp_str = resp.read().decode()
        data = json.loads(resp_str)
        if 'errorMessages' in data:
            raise Exception(data['errorMessages'])
        j = Jira(data, self)
        return j

    def query(self, query):
        jiras = {}
        at = 0
        end = 1
        count = 100
        while at < end:
            params = urllib.parse.urlencode({'jql': query, 'startAt': at, 'maxResults': count})
            # print params
            resp = urllib.request.urlopen(self.baseUrl + "/search?%s" % params)
            resp_str = resp.read().decode()
            data = json.loads(resp_str)
            if 'errorMessages' in data:
                raise Exception(data['errorMessages'])
            at = data['startAt'] + data['maxResults']
            end = data['total']
            for item in data['issues']:
                j = Jira(item, self)
                jiras[j.get_id()] = j
        return jiras

    def unresolved_jiras(self, project):
        """
        :param project: The JIRA project to search for unresolved issues
        :return: All JIRA issues that have the field resolution = Unresolved
        """
        return self.query(f"project = {project} AND resolution = Unresolved")

    def open_jiras(self, project):
        """
        :param project: The JIRA project to search for open issues
        :return: All JIRA issues that have the field status = Open
        """
        return self.query(f"project = {project} AND status = Open")

    def in_progress_jiras(self, project):
        """
        :param project: The JIRA project to search for In Progress issues
        :return: All JIRA issues that have the field status = 'In Progress'
        """
        return self.query(f"project = {project} AND status = 'In Progress'")
