# Release

This document includes information about the Storm release process.

---

# Release Policy

Apache Storm follows the basic idea of [Semantic Versioning](https://semver.org/). Given a version number MAJOR.MINOR.PATCH, increment the:
 1. MAJOR version when you make incompatible API changes,
 2. MINOR version when you add functionality in a backwards compatible manner, and
 3. PATCH version when you make backwards compatible bug fixes.
 
# Release process

## Preparation

Ensure you can log in to http://repository.apache.org. You should use your Apache ID username and password.

Install an svn client, and ensure you can access the https://dist.apache.org/repos/dist/dev/storm/ and https://dist.apache.org/repos/dist/release/storm/ repositories. You should be able to access these with your Apache ID username and password.

Ensure you have a signed GPG key, and that the GPG key is listed in the Storm KEYS file at https://dist.apache.org/repos/dist/release/storm/KEYS. The key should be hooked into the Apache web of trust. You should read the [Apache release signing page](http://www.apache.org/dev/release-signing.html), the [release distribution page](http://www.apache.org/dev/release-distribution.html#sigs-and-sums), as well as the [release publishing](http://www.apache.org/dev/release-publishing) and [release policy](http://www.apache.org/legal/release-policy.html) pages.

If you are setting up a new MINOR version release, create a new branch based on `master` branch, e.g. `2.2.x-branch`. Then on master branch, set the version to a higher MINOR version (with SNAPSHOT), e.g. `mvn versions:set -DnewVersion=2.3.0-SNAPSHOT -P dist,rat,externals,examples`.
In this way, you create a new release line and then you can create PATCH version releases from it, e.g. `2.2.0`.

## Setting up a vote

0. Checkout to the branch to be released.

1. Run `mvn release:prepare -P dist,rat,externals,examples` followed `mvn release:perform -P dist,rat,externals,examples`. This will create all the artifacts that will eventually be available in maven central. This step may seem simple, but a lot can go wrong (mainly flaky tests). 
Note that this will create and push two commits with the commit message starting with "[maven-release-plugin]" and it will also create and publish a git tag, e.g. `v2.2.0`.

2. Once you get a successful maven release, a “staging repository” will be created at http://repository.apache.org in the “open” state, meaning it is still writable. You will need to close it, making it read-only. You can find more information on this step [here](www.apache.org/dev/publishing-maven-artifacts.html).

3. Checkout to the git tag that was published by Step 1 above, e.g. `git checkout tags/v2.2.0 -b v2.2.0`. Run `mvn package` for `storm-dist/binary` and `storm-dist/source` to create the actual distributions.

4. Generate checksums for the *.tar.gz and *.zip distribution files, e.g.
```bash
cd storm-dist/source/target
gpg --print-md SHA512 apache-storm-2.2.0-src.zip > apache-storm-2.2.0-src.zip.sha512
gpg --print-md SHA512 apache-storm-2.2.0-src.tar.gz > apache-storm-2.2.0-src.tar.gz.sha512

cd storm-dist/binary/final-package/target
gpg --print-md SHA512 apache-storm-2.2.0.zip > apache-storm-2.2.0.zip.sha512
gpg --print-md SHA512 apache-storm-2.2.0.tar.gz > apache-storm-2.2.0.tar.gz.sha512
```

5. Create a directory in the dist svn repo for the release candidate: https://dist.apache.org/repos/dist/dev/storm/apache-storm-x.x.x-rcx

6. Run `dev-tools/release_notes.py` for the release version, piping the output to a RELEASE_NOTES.html file. Move that file to the svn release directory, sign it, and generate checksums, e.g.
```bash
python dev-tools/release_notes.py 2.2.0 > RELEASE_NOTES.html
gpg --armor --output RELEASE_NOTES.html.asc --detach-sig RELEASE_NOTES.html
gpg --print-md SHA512 RELEASE_NOTES.html > RELEASE_NOTES.html.sha512
```

7. Move the release files from Step 4 and 6 to the svn directory from Step 5. Add and commit the files. This makes them available in the Apache staging repo.

8. Start the VOTE thread. The vote should follow the [ASF voting process](https://www.apache.org/foundation/voting.html).

## Releasing if the vote succeeds

1. `svn mv https://dist.apache.org/repos/dist/dev/storm/apache-storm-x.x.x-rcx https://dist.apache.org/repos/dist/release/storm/apache-storm-x.x.x`. This will make the release artifacts available on dist.apache.org and the artifacts will start replicating to mirrors.

2. Go to http://repository.apache.org and release the staging repository

3. Wait at least 24 hrs. for the mirrors to catch up.

4. Check out the [storm-site](https://github.com/apache/storm-site) repository, and follow the README to generate release specific documentation for the site. Compose a new blog post announcement for the new release. Update the downloads page. Finally commit and push the site as described in the storm-site README to publish the site.

5. Announce the new release to dev@storm.apache.org, user@storm.apache.org, and announce@apache.org. You will need to use your @apache.org email to do this.

6. Delete any outdated releases from the https://dist.apache.org/repos/dist/release/storm/ repository. See [when to archive](http://www.apache.org/legal/release-policy.html#when-to-archive). 

7. Delete any outdated releases from the storm-site releases directory, and republish the site.

8. Tweet, promote, celebrate. ;)

## Cleaning up if the vote fails

1. Go to http://repository.apache.org and drop the staging repository.

2. Delete the staged distribution files from https://dist.apache.org/repos/dist/dev/storm/

3. Delete the git tag.

# How to vote on a release candidate

We encourage everyone to review and vote on a release candidate to make an Apache Storm release more reliable and trustworthy.

Below is a checklist that one could do to review a release candidate. 
Please note this list is not exhaustive and only includes some of the common steps. Feel free to add your own tests.

1. Verify files such as *.asc, *.sha512; some scripts are available under `dev-tools/rc` to help with it;
2. Build Apache Storm source code and run unit tests, create an Apache Storm distribution;
3. Set up a standalone cluster using apache-storm-xxx.zip, apache-storm-xxx.tar.gz, the Apache Storm distribution created from step 2, separately;
4. Launch WordCountTopology and ThroughputVsLatency topology and check logs, UI metrics, etc;
5. Test basic UI functionalities such as jstack, heap dump, deactivate, activate, rebalance, change log level, log search, kill topology;
6. Test basic CLI such as kill, list, deactivate, deactivate, rebalance, etc.

It's also preferable to set up a standalone secure Apache Storm cluster and test basic funcionalities on it.

Don't feel the pressure to do everything listed above. After you finish your review, reply to the corresponding email thread with your vote, summarize the work you have performed and elaborate the issues
you have found if any. Also please feel free to update the checklist if you think anything important is missing there. 

Your contribution is very much appreciated.  
