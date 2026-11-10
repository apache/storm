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

- We strongly encourage you to read the [Apache release signing page](https://www.apache.org/dev/release-signing.html), the [release distribution page](https://www.apache.org/dev/release-distribution.html#sigs-and-sums), as well as the [release publishing](https://www.apache.org/dev/release-publishing), [release policy](https://www.apache.org/legal/release-policy.html) and [Maven publishing](https://infra.apache.org/publishing-maven-artifacts.html) pages. ASF has common guidelines that apply to all projects.
- Ensure you can log in to https://repository.apache.org. You should use your Apache ID username and password.
- Install a SVN client, and ensure you can access the https://dist.apache.org/repos/dist/dev/storm/ and https://dist.apache.org/repos/dist/release/storm/ repositories. You should be able to access these with your Apache ID username and password. You need this to maintain the KEYS file, and to stage and publish the release through SVN. If you stage and publish through ATR instead, see [Using Apache Trusted Releases (ATR)](#using-atr).
- During the release phase, artifacts will be uploaded to https://repository.apache.org. This means Maven needs to know your LDAP credentials. It is recommended that you use Maven's mechanism for [password encryption](https://maven.apache.org/guides/mini/guide-encryption.html). Please configure this in your `${user.home}/.m2/settings.xml`:
  
```xml
    <settings>
  ...
    <servers>
      <!-- To publish a snapshot of your project -->
      <server>
        <id>apache.snapshots.https</id>
        <username> <!-- YOUR APACHE LDAP USERNAME --> </username>
        <password> <!-- YOUR APACHE LDAP PASSWORD (encrypted) --> </password>
      </server>
      <!-- To stage a release of your project -->
      <server>
        <id>apache.releases.https</id>
        <username> <!-- YOUR APACHE LDAP USERNAME --> </username>
        <password> <!-- YOUR APACHE LDAP PASSWORD (encrypted) --> </password>
      </server>
     ...
    </servers>
  </settings>
``` 
- Ensure you have a signed GPG key, and that the GPG key is listed in the Storm KEYS file at https://dist.apache.org/repos/dist/release/storm/KEYS. The key should be hooked into the Apache web of trust (https://keyserver.ubuntu.com for example)
   - set up the key as the default one to be used during the signing operations that the GPG Maven plugin will request (your OS GPG agent can do this)
   - If your key is not yet in the KEYS file, add it:
     ```bash
     svn checkout https://dist.apache.org/repos/dist/release/storm/
     gpg --export --armor <YOUR_KEY_ID> >> storm/KEYS
     svn commit storm/KEYS -m "Add GPG key for release manager <YOUR NAME>"
     ```
- Compile environment:
  - some tests currently rely on the following packages being available locally:
    - NodeJS
    - Python3
  - some tests will require Docker to be running since they will create/launch containers (make sure `/var/run/docker.sock` has the correct permissions, otherwise you might see the error `Could not find a valid Docker environment`)

If you are setting up a new MINOR version release, create a new branch based on `master` branch, e.g. `2.7.x-branch`. Then on master branch, set the version to a higher MINOR version (with SNAPSHOT), e.g. `mvn versions:set -DnewVersion=2.8.0-SNAPSHOT -P dist,rat,externals,examples`.
In this way, you create a new release line and then you can create PATCH version releases from it, e.g. `2.7.1`.

<a id="using-atr"></a>
## Using Apache Trusted Releases (ATR)

[ATR](https://releases.apache.org/) is the ASF platform for staging, voting on and publishing releases. It replaces the `dist/dev`
staging area, sends the vote and announcement emails on your behalf, and publishes the approved files to `dist/release`. It is currently
in beta. ATR does **not** build or sign the artifacts and does **not** publish to Maven Central: building, signing and the Nexus staging
repository work as described in the steps below.

> **Pick one path for each release, ATR or SVN, and never mix them.** The two paths diverge at staging (step 6 of
> [Setting up a vote](#setting-up-a-vote)), at the vote email (step 11), and again when publishing and announcing after the vote.
> Decide before you start and follow only the matching variant in every step. Mixing them, for example staging in `dist/dev` and also
> starting the release in ATR, or running `svn mv` into `dist/release` while ATR holds the release, leaves two diverging copies of the
> release candidate. Inside ATR you are free to switch between the website and the command line.

Every ATR step can be done either on the **website** (https://releases.apache.org, log in with your ASF account) or from the
**command line** with the `atr` client. The steps below give both. Voting still happens on dev@storm.apache.org,
and only votes from the Storm PMC are binding. Staging and publishing through SVN without ATR still works; the SVN variants are marked in the steps below.

### One-time setup

1. Register for the ASF multi-factor authentication at https://mfa.apache.org/.
2. You must be a PMC member, or be designated as a release manager by a PMC member on the Storm committee page in ATR.
3. Make sure ATR knows your OpenPGP signing key. By default ATR imports the Storm KEYS file from SVN, so adding your key to KEYS as described in [Preparation](#preparation)
   is enough. You can also add the key at https://releases.apache.org/keys. New keys must be RSA of at least 4096 bits, or Ed25519 / ECDSA (P-256 or stronger); DSA keys are rejected.
4. Get the credentials for the way you upload files:
   - website: nothing more than your ASF login;
   - `rsync`: add an SSH **public** key at https://releases.apache.org/keys. This is a different key from your OpenPGP key. You connect as your ASF UID on port 2222;
   - command line: create a personal access token at https://releases.apache.org/tokens (it expires after 180 days).
5. To use the command line, install the client (it needs [uv](https://docs.astral.sh/uv/getting-started/installation/)) and sign in:

   ```bash
   uv tool install "apache-trusted-releases @ git+https://github.com/apache/tooling-releases-client"
   atr set asf.uid <your-asf-uid>
   atr set tokens.pat
   atr jwt refresh
   ```

   `atr set tokens.pat` asks for the token without echoing it. Run `atr --help` or `atr docs` for the full command reference. The client is not yet stable, so do not use it in unattended scripts.
6. Log in on the website and check that the Storm project is listed. Its project key is `storm`.
7. The vote email template lives in this repository, in [`dev-tools/atr/start-vote-template.txt`](dev-tools/atr/start-vote-template.txt). Once, in the project settings in ATR
   (**Vote** tab), set **Start vote template URL** to
   `https://raw.githubusercontent.com/apache/storm/master/dev-tools/atr/start-vote-template.txt`. ATR fetches the template whenever the vote form is shown, so changes to
   the file in `master` apply to the next vote. The file uses ATR's `{{VARIABLES}}`, listed under **Available template variables** on the same settings page.
   It is sent as the body of the email, so it must not carry a license header.

### ATR operations at a glance

| Step | Website | Command line (`x.x.x` is the release version) |
|---|---|---|
| Start the release | **Draft → Candidates → Start a new release** | `atr release start storm x.x.x` |
| Upload the files | Compose page → **Upload files** (browser, `svn` or `rsync`) | `atr rsync storm x.x.x ./candidate/` or `atr upload ...` |
| Review the checks | Compose page | `atr check wait\|status\|blockers\|concerns storm x.x.x` |
| Start the vote | Compose page → **Start voting** | `atr vote start storm x.x.x -m dev@storm.apache.org --subject ... --body-file vote.txt` |
| Read the tally | Vote page | `atr vote tabulate storm x.x.x` |
| Resolve the vote | Vote page → **Resolve vote** (the only way for email votes) | not available for email votes |
| Record the Maven Central release | Finish page → **Verify a third-party distribution** | `atr distribution record storm x.x.x MAVEN org.apache.storm <artifactId> x.x.x` |
| Publish to `dist/release` | Finish page → **Publish to ASF Distribution Area** | add `--auto-publish` to `vote start` (PMC members only) |
| Announce | Finish page → **Announce** | `atr announce storm x.x.x -m announce@apache.org --body-file announce.txt` |

<a id="setting-up-a-vote"></a>
## Setting up a vote

> **Choose your path first: ATR or SVN, not both.** Steps marked **ATR** and **SVN** are alternatives. Follow the variant of the path you chose and skip the other.
> See [Using Apache Trusted Releases (ATR)](#using-atr).

1. Checkout to the branch to be released.

2. <a id="rc-maven-release"></a>Run `mvn release:prepare -P dist,rat,externals,examples` followed by `mvn release:perform -P dist,rat,externals,examples`.
   This will create all the artifacts that will eventually be available in maven central. This step may seem simple,
   but a lot can go wrong (mainly flaky tests). Note that this will create and push two commits with the commit message
   starting with "[maven-release-plugin]" and it will also create and publish a git tag, e.g. `v2.8.1`. Note: the full build can take up to 30 minutes to complete.

   If the build fails mid-way, run `mvn release:clean` to remove generated files before retrying.

3. Once you get a successful maven release, a "staging repository" will be created at https://repository.apache.org
   in the "open" state, meaning it is still writable. You will need to close it, making it read-only. You can find more
   information on this step [here](https://infra.apache.org/publishing-maven-artifacts.html).

   Note the staging repository ID (e.g. `orgapachestorm-1234`) shown in the Maven console output, or find it in the
   Nexus UI under **Staging Repositories** at https://repository.apache.org/#stagingRepositories. You will need this
   ID in the vote email template.

4. <a id="rc-checkout-tag"></a>Checkout to the git tag that was published by [the Maven release](#rc-maven-release) above, e.g. `git checkout tags/v2.8.1 -b v2.8.1`.
   Then build and package the distributions:

   ```bash
   mvn clean install -DskipTests -P dist
   cd storm-dist/binary && mvn package && cd ../..
   cd storm-dist/source && mvn package && cd ../..
   ```

5. <a id="rc-checksums"></a>Generate checksums for the *.tar.gz and *.zip distribution files, e.g.

   > **macOS note:** use `shasum -a 512` in place of `sha512sum`.

   > The Maven build may already have generated `.sha512` files in the target directories; verify they exist before
   > running the commands below.

   ```bash
   pushd storm-dist/source/target
   sha512sum apache-storm-2.8.1-src.zip > apache-storm-2.8.1-src.zip.sha512
   sha512sum apache-storm-2.8.1-src.tar.gz > apache-storm-2.8.1-src.tar.gz.sha512
   popd

   pushd storm-dist/binary/final-package/target
   sha512sum apache-storm-2.8.1.zip > apache-storm-2.8.1.zip.sha512
   sha512sum apache-storm-2.8.1.tar.gz > apache-storm-2.8.1.tar.gz.sha512
   sha512sum apache-storm-2.8.1-lite.zip > apache-storm-2.8.1-lite.zip.sha512
   sha512sum apache-storm-2.8.1-lite.tar.gz > apache-storm-2.8.1-lite.tar.gz.sha512
   popd
   ```

   > **Note:** the binary build produces two distributions. The full one
   > (`apache-storm-x.x.x.tar.gz` / `.zip`) bundles the optional `storm-autocreds` and
   > `storm-kafka-monitor` jars, while the lean one (`apache-storm-x.x.x-lite.tar.gz` / `.zip`)
   > omits them. Both must be signed, checksummed and staged for the release candidate.
   > See [Storm lite distribution](#storm-lite-distribution) below.

6. <a id="rc-svn-dir"></a>Decide where the release candidate is staged.

   - **ATR:** start the release, on the website (**Draft → Candidates → Start a new release**) or from the command line: `atr release start storm x.x.x`.
     A failed vote returns the release to the Compose phase, so the version stays the same and the `rcN` counter only appears in the vote email.
   - **SVN:** create a directory in the dist svn repo for the release candidate: https://dist.apache.org/repos/dist/dev/storm/apache-storm-x.x.x-rcx

7. Before generating the release notes, please double check if all merged pull requests for the version being released are assigned the milestone in question. They won't be placed in the release notes otherwise.

8. <a id="rc-release-notes"></a>Run `dev-tools/release_notes.py` for the release version, piping the output to a RELEASE_NOTES.html file. Move that file to the svn release directory, sign it, and generate checksums, e.g.

   ```bash
   export GITHUB_TOKEN=<your-github-pat>
   python3 dev-tools/release_notes.py <id-of-the-github-milestone> > RELEASE_NOTES.html
   gpg --armor --output RELEASE_NOTES.html.asc --detach-sig RELEASE_NOTES.html
   sha512sum RELEASE_NOTES.html > RELEASE_NOTES.html.sha512
   ```

   To create a personal access token:

   - Go to your GitHub account settings.
   - Navigate to **Developer Settings** > **Personal Access Tokens** > **Tokens (classic)**.
   - Generate a token with the `public_repo` scope.

   To obtain the ID of a GitHub milestone:

   - Visit the [milestone overview](https://github.com/apache/storm/milestones).
   - Click on the milestone you want to create release notes for.
   - Look at the URL in your browser. It will look like this: `https://github.com/apache/storm/milestone/40`, where the last number is the milestone ID.

9. Move the release files from the [tag checkout](#rc-checkout-tag), [checksums](#rc-checksums) and [release notes](#rc-release-notes) steps to the [staging location](#rc-svn-dir): a local directory such as `./candidate/` for ATR, or the dist svn directory for SVN. Example of the set of files:

   ```
   apache-storm-2.8.3-src.tar.gz         apache-storm-2.8.3-src.zip         apache-storm-2.8.3.tar.gz         apache-storm-2.8.3.zip         RELEASE_NOTES.html
   apache-storm-2.8.3-src.tar.gz.asc     apache-storm-2.8.3-src.zip.asc     apache-storm-2.8.3.tar.gz.asc     apache-storm-2.8.3.zip.asc     RELEASE_NOTES.html.asc
   apache-storm-2.8.3-src.tar.gz.sha512  apache-storm-2.8.3-src.zip.sha512  apache-storm-2.8.3.tar.gz.sha512  apache-storm-2.8.3.zip.sha512  RELEASE_NOTES.html.sha512
   apache-storm-2.8.3-lite.tar.gz        apache-storm-2.8.3-lite.zip
   apache-storm-2.8.3-lite.tar.gz.asc    apache-storm-2.8.3-lite.zip.asc
   apache-storm-2.8.3-lite.tar.gz.sha512 apache-storm-2.8.3-lite.zip.sha512
   ```

10. Upload or commit the files.

    - **ATR:** upload on the website (Compose page → **Upload files**, by browser, `svn` or `rsync`) or from the command line:

      ```bash
      atr rsync storm x.x.x ./candidate/
      ```

      `rsync` needs an SSH public key registered at https://releases.apache.org/keys, and connects as your ASF UID on port 2222. Then wait for the checks
      on the Compose page, or run:

      ```bash
      atr check wait storm x.x.x --timeout 300
      atr check status storm x.x.x --verbose
      atr check blockers storm x.x.x --members
      atr check concerns storm x.x.x --members
      ```

      Fix every blocker (re-sign, re-checksum and re-upload the affected files). Read every concern: you acknowledge the remaining ones when you start the vote.
      ATR also asks for the full 40-character commit the sources were built from, which is `git rev-parse vx.x.x^{commit}`.
      Expect the concern `RELEASE_NOTES.html: Unknown top level file`: ATR only accepts `CHANGES`, `LICENSE`, `NOTICE`, `README` and `SECURITY` (with an empty, `.adoc`, `.md`, `.rst` or `.txt` suffix) at the top level of a candidate.
    - **SVN:** add and commit the files to SVN. This makes them available in the Apache staging repo.

      ```bash
      cd <your-svn-checkout>/apache-storm-x.x.x-rcx
      svn add *
      svn commit -m "Add release candidate apache-storm-x.x.x-rcx"
      ```

11. Start the VOTE thread. The vote should follow the [ASF voting process](https://www.apache.org/foundation/voting.html).

    - **ATR:** start the vote on the website (Compose page → **Start voting**) or from the command line. ATR sends the email to the list you choose, in your name.
      On the website the form is filled in from [`dev-tools/atr/start-vote-template.txt`](dev-tools/atr/start-vote-template.txt) (see [One-time setup](#using-atr)),
      with the ATR variables replaced. Replace `[REPO_NUM]` with the number of the Nexus staging repository, review the text, and send. This is the easier way for Storm,
      because ATR fills in the variables for you.
      From the command line, write the whole email body yourself, with every value filled in (use the template file as the starting point), and pass the subject line separately:

      ```bash
      atr vote start storm x.x.x -m dev@storm.apache.org -d 72 --subject "[VOTE] Release Apache Storm x.x.x (rcN)" --body-file vote.txt
      ```

      Add `--concerns-noted <keys>` to acknowledge the remaining concerns. `--auto-publish` (PMC members only) publishes the files as soon as the vote is resolved;
      leave it out if you want to control when that happens.
    - **SVN:** send the email yourself, using the sample template below.

    *Sample template for an SVN vote, sent to dev@storm.apache.org:*

    ```
    Subject: [VOTE] Release Apache Storm [VERSION] (rcN)

    Hi folks,

    I have posted a [Nth] release candidate for the Apache Storm [VERSION] release and it is ready for testing.

    The Nexus staging repository is here:
        https://repository.apache.org/content/repositories/orgapachestorm-[REPO_NUM]

    Storm Source and Binary Release with sha512 signature files are here:
        https://dist.apache.org/repos/dist/dev/storm/apache-storm-[VERSION]-rcN/
    The release artifacts are signed with the following key:
        https://keyserver.ubuntu.com/pks/lookup?op=index&fingerprint=on&search=[KEY]
        in this file https://downloads.apache.org/storm/KEYS

    The release was made from the Apache Storm [VERSION] tag at:
        https://github.com/apache/storm/tree/v[VERSION]

    Full list of changes in this release:
        https://dist.apache.org/repos/dist/dev/storm/apache-storm-[VERSION]-rcN/RELEASE_NOTES.html

    To use it in a maven build set the version for Storm to [VERSION] and add the following URL to your settings.xml file:
    https://repository.apache.org/content/repositories/orgapachestorm-[REPO_NUM]

    The release was made using the Storm release process, documented on the GitHub repository:
    https://github.com/apache/storm/blob/master/RELEASING.md

    Please vote on releasing these packages as Apache Storm [VERSION]. The vote is open for at least the next 72 hours.
    "How to vote" is described here: https://github.com/apache/storm/blob/master/RELEASING.md#how-to-vote-on-a-release-candidate
    When voting, please list the actions taken to verify the release.

    Only votes from the Storm PMC are binding, but everyone is welcome to check the release candidate and vote.
    The vote passes if at least three binding +1 votes are cast.

    [ ] +1 Release this package as Apache Storm [VERSION]
    [ ]  0 No opinion
    [ ] -1 Do not release this package because...

    Thanks to everyone who contributed to this release.

    Thanks!
    [Release Manager Name]
    ```

## Storm lite distribution

Since 3.0.0 the binary build produces two distributions from `storm-dist/binary`:

| Artifact | Contents |
|---|---|
| `apache-storm-x.x.x.tar.gz` / `.zip` | Full distribution. Bundles the `storm-autocreds` (Hadoop/HBase) and `storm-kafka-monitor` (Kafka client) jars. |
| `apache-storm-x.x.x-lite.tar.gz` / `.zip` | Lean distribution. Ships only the READMEs for those two plugins; everything else is identical. |

Both are produced by the same `mvn package` run in `storm-dist/binary` (assembly
executions `bin` and `lite`), and both are automatically signed by the GPG plugin.
The lite artifact is attached with the `lite` Maven classifier.

Release managers must sign, checksum and stage **both** distributions, and list
both in the release candidate VOTE mail so reviewers know which to verify.

Users of the lite distribution can install the omitted plugins on demand with
`bin/storm-autocreds-fetch` and `bin/storm-kafka-monitor-fetch`, which resolve them
from Maven Central (or an internal mirror configured in `settings.xml`).


## Releasing if the vote succeeds

> Continue on the path you chose when you [set up the vote](#setting-up-a-vote): the **ATR** and **SVN** variants below are alternatives.

0. Announce the results.

   - **ATR:** check ATR's tally against the vote thread, then resolve the vote on the vote page (**Resolve vote**; this is the only way to resolve an email vote).
     `atr vote tabulate storm x.x.x` shows the tally from the command line. ATR emails the outcome to the list, and a passing vote moves the release to the Finish phase.
   - **SVN:** use the following template:

   ```text
   Subject: [VOTE][RESULT] Storm [VERSION] Release Candidate [N]

   Dear Community,

   The voting for releasing Apache Storm [VERSION] Release Candidate [N] has passed with # +1 votes (# binding) and # +0 or -1 votes.
   +1 votes:
   * ###### / binding
   * ############

   Vote thread can be found at:
   https://lists.apache.org/thread.html/##############

   Thanks everyone for taking the time to review and vote for the release!
   We will continue the rest of release process and send out the announcement email in the coming days.

   Thanks to everyone who contributed to this release.

   [RELEASE MANAGER NAME]
   ```

1. Publish the release candidate to the release directory.

   - **ATR:** on the Finish page use **Publish to ASF Distribution Area**, and set the download path suffix to `apache-storm-x.x.x` so the files end up where they always have.
     If you started the vote with `--auto-publish`, ATR has already done this when the vote was resolved.
   - **SVN:**

     ```bash
     svn mv https://dist.apache.org/repos/dist/dev/storm/apache-storm-x.x.x-rcx \
            https://dist.apache.org/repos/dist/release/storm/apache-storm-x.x.x \
            -m "Publish Apache Storm x.x.x release"
     ```

   Either way, this will make the release artifacts available on dist.apache.org and the artifacts will start replicating to mirrors.

2. Go to https://repository.apache.org and release the staging repository.

   **ATR:** then record the Maven Central release in ATR, on the Finish page (**Verify a third-party distribution**) or from the command line, once per artifact:

   ```bash
   atr distribution record storm x.x.x MAVEN org.apache.storm <artifactId> x.x.x
   ```

   ATR can require this before it lets you announce.

3. Wait at least 24 hrs. for the mirrors to catch up.

4. Check out the [storm-site](https://github.com/apache/storm-site) repository, and follow the README to generate release specific documentation for
   the site. Compose a new blog post announcement for the new release. Update the downloads page. Finally, commit and push
   the site as described in the storm-site README to publish the site.

5. Update `doap_Storm.rdf` with the new release version.

6. Delete any outdated releases from the https://dist.apache.org/repos/dist/release/storm/ repository. See [when to archive](https://www.apache.org/legal/release-policy.html#when-to-archive).
   With ATR, you can instead archive the superseded release from its page in ATR.

7. Delete any outdated releases from the storm-site releases directory, and republish the site.

8. Create a release on [GitHub](https://github.com/apache/storm/releases). Generate the release notes with the GitHub tooling.

9. Close the milestone for the released version in GitHub. Go to the [milestones page](https://github.com/apache/storm/milestones), open the milestone matching the released version (e.g. `3.1.0`), and click **Close milestone**. The milestone will already have no open issues at this point -- generating the release notes with `dev-tools/release_notes.py` refuses to run while any remain -- so this is simply the final housekeeping step.

10. Create a new release for [Storm Docker](https://github.com/apache/storm-docker). Example of a version release [here](https://github.com/apache/storm-docker/commit/177a1534bf910c2271845f4eaedef7c040559fbc). After that is done, a PR to [docker-library](https://github.com/docker-library/official-images) must be submitted, so that the new docker-storm version is officially released. Example of such a PR is [here](https://github.com/docker-library/official-images/pull/21525#issuecomment-4526751672).

11. Post, promote, celebrate. ;) Announce the new release to dev@storm.apache.org, user@storm.apache.org, and announce@apache.org (you must use your @apache.org email to post to announce@apache.org), using the following template.

    With ATR, you can send the announcement from ATR once the files have reached the download servers: on the Finish page (**Announce**) or from the command line,
    with the template below in a file as the body (`--cc` and `--bcc` add other permitted lists):

    ```bash
    atr announce storm x.x.x -m announce@apache.org --body-file announce.txt
    ```

    With SVN, send it yourself.

    ```text
    Subject: [ANNOUNCE] Apache Storm [VERSION] Released

    The Apache Storm community is pleased to announce the release of Apache
    Storm version [VERSION].

    Apache Storm is a distributed, fault-tolerant, and high-performance
    realtime computation system that provides strong guarantees on the
    processing of data. You can read more about Apache Storm on the project
    website:

    https://storm.apache.org/

    Downloads of source and binary distributions are listed in our download
    section:

    https://storm.apache.org/downloads.html

    You can read more about this release in the following blog post:

    https://storm.apache.org/[YEAR]/[MONTH]/[DAY]/storm[VERSION]-released.html

    Distribution artifacts are available in Maven Central at the following
    coordinates:

    groupId: org.apache.storm
    artifactId: storm-{component}
    version: [VERSION]

    The full list of changes is available here [1]. Please let us know [2] if
    you encounter any problems.

    Regards,
    The Apache Storm Team

    [1] https://downloads.apache.org/storm/apache-storm-[VERSION]/RELEASE_NOTES.html
    [2] https://github.com/apache/storm/issues
    ```

> **Don't forget:** when the release was cut from a release branch, update `master` with the
> new version afterwards — bump `master` to the appropriate next development (`-SNAPSHOT`) version
> and bring over any release-branch changes that also belong on `master`.

## Cleaning up if the vote fails

1. Go to https://repository.apache.org and drop the staging repository.

2. Clean up the staged files.

   - **ATR:** nothing to delete in `dist`. Resolve the vote as failed on the vote page; the release returns to the Compose phase, where you upload the next candidate.
   - **SVN:** delete the staged distribution files from https://dist.apache.org/repos/dist/dev/storm/.

3. Delete the git tag.

4. Send a [VOTE][CANCELED] message to dev@storm.apache.org using the following format:

   ```text
   Subject: [VOTE][CANCELED] Storm [VERSION] Release Candidate [N]

   This release candidate Storm Release candidate [VERSION] rcN https://dist.apache.org/repos/dist/dev/storm/apache-storm-[VERSION]-rcN/ has been canceled.
   New vote request will be sent out on RC[N+1] with further updates.

   [RELEASE MANAGER NAME]
   ```

# How to vote on a release candidate

We encourage everyone to review and vote on a release candidate to make an Apache Storm release more reliable and trustworthy.

Below is a checklist that one could do to review a release candidate. 
Please note this list is not exhaustive and only includes some of the common steps. Feel free to add your own tests.

0. Get the candidate files from the link in the vote email: a `dist.apache.org` directory for an SVN vote, or the vote page for an ATR vote, which also shows the check results. On the ATR vote page anyone can download the files in the browser or with `curl`; the ZIP download and `rsync` need a login.

1. Verify files such as *.asc, *.sha512; some scripts are available under `dev-tools/rc` to help with it;
2. <a id="vote-build-distribution"></a>Build Apache Storm source code and run unit tests, create an Apache Storm distribution;
3. Set up a standalone cluster using apache-storm-xxx.zip, apache-storm-xxx.tar.gz, the Apache Storm distribution created in [the build step](#vote-build-distribution), separately;
4. Launch WordCountTopology and ThroughputVsLatency topology and check logs, UI metrics, etc;
5. Test basic UI functionalities such as jstack, heap dump, deactivate, activate, rebalance, change log level, log search, kill topology;
6. Test basic CLI such as kill, list, deactivate, activate, rebalance, etc.

It's also preferable to set up a standalone secure Apache Storm cluster and test basic functionalities on it.

Don't feel the pressure to do everything listed above. After you finish your review, reply to the corresponding email thread with your vote, summarize the work you have performed and elaborate the issues
you have found if any. Also please feel free to update the checklist if you think anything important is missing there. 

Your contribution is very much appreciated.  
