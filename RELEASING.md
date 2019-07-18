# Committer documentation

This document summarizes information relevant to Storm committers.  It includes information about
the Storm release process.

---

# Release process

## Preparation

Ensure you can log in to http://repository.apache.org. You should use your Apache ID username and password.

Install an svn client, and ensure you can access the https://dist.apache.org/repos/dist/dev/storm/ and https://dist.apache.org/repos/dist/release/storm/ repositories. You should be able to access these with your Apache ID username and password.

Ensure you have a signed GPG key, and that the GPG key is listed in the Storm KEYS file at https://dist.apache.org/repos/dist/release/storm/KEYS. The key should be hooked into the Apache web of trust. You should read the [Apache release signing page](http://www.apache.org/dev/release-signing.html), the [release distribution page](http://www.apache.org/dev/release-distribution.html#sigs-and-sums), as well as the [release publishing](http://www.apache.org/dev/release-publishing) and [release policy](http://www.apache.org/legal/release-policy.html) pages.

## Setting up a vote

1. Run `mvn release:prepare` followed `mvn release:perform` on the branch to be released. This will create all the artifacts that will eventually be available in maven central. This step may seem simple, but a lot can go wrong (mainly flaky tests).

2. Once you get a successful maven release, a “staging repository” will be created at http://repository.apache.org in the “open” state, meaning it is still writable. You will need to close it, making it read-only. You can find more information on this step [here](www.apache.org/dev/publishing-maven-artifacts.html).

3. Run `mvn package` for `storm-dist/binary` and `storm-dist/source` to create the actual distributions.

4. Sign and generate checksums for the *.tar.gz and *.zip distribution files. 

5. Create a directory in the dist svn repo for the release candidate: https://dist.apache.org/repos/dist/dev/storm/apache-storm-x.x.x-rcx

6. Run `dev-tools/release_notes.py` for the release version, piping the output to a RELEASE_NOTES.html file. Move that file to the svn release directory, sign it, and generate checksums.

7. Move the release files from Step 4 and 6 to the svn directory from Step 5. Add and commit the files. This makes them available in the Apache staging repo.

8. Start the VOTE thread. The vote should follow the [ASF voting process](https://www.apache.org/foundation/voting.html).

## Releasing if the vote succeeds

1. `svn mv https://dist.apache.org/repos/dist/dev/storm/apache-storm-x.x.x-rcx https://dist.apache.org/repos/release/dev/storm/apache-storm-x.x.x`. This will make the release artifacts available on dist.apache.org and the artifacts will start replicating to mirrors.

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
