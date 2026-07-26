# lib-worker

Drop-in directory for worker-only jars.

Jars placed here are added to the classpath of every worker JVM launched by
the supervisor, but not to the daemon (nimbus / supervisor / ui) classpath.

The distribution itself ships no worker-only jars: the jars shared by the
daemons and the workers live in `lib-common/`, daemon-only jars in `lib/`.
The classpaths are composed as:

    daemon classpath = lib-common + lib
    worker classpath = lib-common + lib-worker

See also `extlib/`, which is added to both the daemon and worker classpaths.
