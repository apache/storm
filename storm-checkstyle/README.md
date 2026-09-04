# Checkstyle auto-fix

`storm_checkstyle.xml` is the Checkstyle ruleset enforced on every `mvn` build (bound
to the `validate` phase, `violationSeverity=error`, `failOnViolation=true` - a build
fails on any violation).

`src/main/resources/storm/checkstyle-autofix.groovy` auto-corrects a subset of that
ruleset's violations in your source files. It runs via `gmavenplus-plugin`, bound to
`validate` immediately before `maven-checkstyle-plugin`, so by the time Checkstyle
checks your code, the fixable violations are already gone.

The fixer reads its parameters (indent width, import group order, etc.) straight out
of the active ruleset XML at build time via `XmlSlurper` - it has no formatter engine
or preset style of its own. If you change the ruleset, the fixer's behavior changes
with it, with no code change required.

## What it fixes

| Checkstyle module | Fix |
|---|---|
| `FileTabCharacter` | Tabs replaced with `basicOffset` spaces |
| `NoWhitespaceBeforeCaseDefaultColon` | `case FOO :` -> `case FOO:` |
| `NoWhitespaceBefore` (labeled statements) | `LOOP : for (...)` -> `LOOP: for (...)` |
| `CommentsIndentation` | Standalone comments realigned to the code they attach to |
| `CustomImportOrder` | Imports regrouped/sorted per the ruleset's declared rules |

Everything else Checkstyle flags (line length, missing Javadoc, final parameters,
brace placement, most whitespace rules, ...) is **not** auto-fixed and must still be
corrected by hand - those need real Java-parser-level understanding to fix safely
without risking a wrong edit, or are a style/policy call rather than a mechanical one.

## Running it

### As part of a normal build

Nothing to do - it runs automatically at `validate`:

```sh
mvn clean install
```

### For the whole project, without the rest of the build

Runs the fixer across every module's `compileSourceRoots`/`testCompileSourceRoots`
and stops:

```sh
mvn validate
```

### For a single module

Substitute the module's artifact id (or a relative path via `-f`):

```sh
mvn -pl storm-client org.codehaus.gmavenplus:gmavenplus-plugin:execute@checkstyle-autofix
```

`mvn -pl <module> validate` also works, but additionally runs every other plugin
bound to `validate` for that module (including the Checkstyle check itself, which
will still fail on whatever the fixer doesn't cover) - use the `gmavenplus-plugin`
goal directly if you only want the auto-fix step in isolation.

### Skipping it

To build without running the fixer (e.g. to inspect Checkstyle's raw output, or to
review the fixer's own diff before it runs again):

```sh
mvn <goal> -Dgmavenplus.skip=true
```

### Using a different ruleset

`storm.checkstyle.config` (root `pom.xml` property) names the active ruleset by
basename - both the fixer and `maven-checkstyle-plugin`'s `configLocation` read the
same property, so they never drift out of sync. To point at a sibling ruleset file
in this module's `src/main/resources/storm/` directory:

```sh
mvn <goal> -Dstorm.checkstyle.config=<basename>   # no .xml extension
```

## Standalone auto-fix commands, per module

```sh
mvn -pl storm-client                    org.codehaus.gmavenplus:gmavenplus-plugin:execute@checkstyle-autofix
mvn -pl storm-core                      org.codehaus.gmavenplus:gmavenplus-plugin:execute@checkstyle-autofix
mvn -pl storm-server                    org.codehaus.gmavenplus:gmavenplus-plugin:execute@checkstyle-autofix
mvn -pl external/storm-kafka-client     org.codehaus.gmavenplus:gmavenplus-plugin:execute@checkstyle-autofix
```

(Any module under the reactor works the same way - swap in its path or artifact id.)
