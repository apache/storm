# Checkstyle auto-fix

`storm_checkstyle.xml` is the Checkstyle ruleset enforced on every `mvn` build. 
Many rules are `severity=warning` as they do not fail the build, but they should still be 
slimmed to the minimal amount

The auto-fix is **not** part of the normal build. It is run on demand in two stages, 
in every module that declares the plugins:

1. **`rewrite-maven-plugin` (OpenRewrite)** - reduced amount of fixes done on a parsed Java AST. 
   The recipe list is `rewrite.yml` at the repo root (`org.apache.storm.checkstyle.AutoFix`).
2. **`src/main/resources/storm/checkstyle-autofix.groovy`** (via `gmavenplus-plugin`) -
   runs Checkstyle itself, in-process, with the same version and ruleset the build
   uses, reads violations (file, line, column, message) and hands each one to a small fixer written for the one
   Checkstyle module that reported it. The cycle until no more rules can be fixed.

The values a fixer needs (`Indentation` `basicOffset`, `LineLength` `max`,
`CustomImportOrder` rules) are read from the ruleset XML at run time avoiding hardcoding. Files that the
module excludes from Checkstyle (`<excludes>` of `maven-checkstyle-plugin`, e.g. thrift
generated code) are neither audited nor rewritten.

Safety net, per file: a file that stops parsing after a fix is restored to the state
before that round; a file that ends with more violations than it started with is
restored to its original text.

## What it fixes

| Checkstyle module | Stage |
|---|---|
| `NeedBraces`, `MultipleVariableDeclarations`, `ModifierOrder`, `UpperEll`, `ArrayTypeStyle`, `FileTabCharacter`, `OperatorWrap` | OpenRewrite (dedicated recipes) |
| `WhitespaceAround`, `WhitespaceAfter`, `NoWhitespaceBefore`, `ParenPad`, `MethodParamPad`, `GenericWhitespace` | OpenRewrite (`Spaces`, style `org.apache.storm.checkstyle.Style`) |
| `Indentation`, `EmptyLineSeparator`, `CustomImportOrder` | OpenRewrite (`TabsAndIndents`, `BlankLines`, `OrderImports`, same style) |
| `AvoidStarImport` | OpenRewrite (`RemoveUnusedImports`; needs type attribution, i.e. the module's dependencies resolvable) |
| `MissingSwitchDefault` | OpenRewrite (`DefaultComesLast`: only moves an existing `default` last, it does not add a missing one) |
| `LeftCurly`, `RightCurly`, `NoLineWrap`, `AnnotationLocation` | OpenRewrite (`WrappingAndBraces`), partial: it does not move `{`, `catch`/`finally` or every annotation; what is left is not chased in Groovy |
| `NoWhitespaceBeforeCaseDefaultColon`, `RegexpSinglelineJava` (empty-block spacing) | OpenRewrite (`Spaces`), partial: it writes `{ }` for empty blocks, which `RegexpSinglelineJava` rejects |
| `CommentsIndentation` | OpenRewrite (`TabsAndIndents`), partial |
| `OneStatementPerLine`, `SeparatorWrap` | Groovy (no recipe) |
| `LineLength` | Groovy: re-wraps comments and Javadoc, and breaks code at commas, `&&`/`\|\|`/`+`/`?`, `.method(` chains and `=`, and splits long string literals into `"a" + "b"` |
| `IllegalTokenText`, `AvoidEscapedUnicodeCharacters`, `TodoComment`, single-line comment space | Groovy |
| `OverloadMethodsDeclarationOrder`, `ConstructorsDeclarationGrouping` | Groovy: the member is moved next to its overloads/constructors |
| `JavadocLeadingAsteriskAlign`, `JavadocMissingLeadingAsterisk`, `JavadocContentLocation`, `JavadocParagraph`, `JavadocTagContinuationIndentation`, `RequireEmptyLineBeforeBlockTagGroup`, `AtclauseOrder`, `InvalidJavadocPosition`, `SummaryJavadoc` (missing period, lowercase first word) | Groovy |

The OpenRewrite style mirrors the ruleset by hand (`rewrite.yml`); keep `continuationIndent`
at 8, the convention of the code base (Checkstyle's `lineWrappingIndentation` of 4 is a minimum).

## License headers

`storm-checkstyle/src/main/resources/storm/normalize-license-headers.groovy` rewrites the
leading license comment of the module's Java, Groovy, XML, Markdown, YAML, properties, Python and
shell files to the exact template of
https://www.apache.org/legal/src-headers.html#headers. A file whose leading comment is not an ASF
license header, or that carries a copyright notice of anybody but the ASF (a third-party work), is
left alone:

```sh
mvn -pl <module> org.codehaus.gmavenplus:gmavenplus-plugin:execute@normalize-license-headers
```

Add `-Dlicense.check=true` to only report (the build fails if a file would change).

## Not fixed

The following violations can't be fixed mechanically in a satisfactory way:

- `MissingJavadocMethod`, `MissingJavadocType`, `JavadocMethod`, `NonEmptyAtclauseDescription`,
  `SingleLineJavadoc`, the rest of `SummaryJavadoc` - human intervention for docs.
- `MethodName`, the other naming rules and `AbbreviationAsWordInName` - a rename needs
  whole-repo symbol resolution.
- `FallThrough`, `EmptyCatchBlock` - comments could silence bugs.
- `VariableDeclarationUsageDistance` - moving a declaration can reorder side effects.
- `OneTopLevelClass`, `OuterTypeFilename`, `LeftCurly` on `case X: {`,
  `TextBlockGoogleStyleFormatting`.
- `LineLength` on lines with no safe break point (long unbreakable tokens, URLs).

### Partial OpenRewrite coverage

`WrappingAndBraces` (custom `WrappingAndBracesStyle`) does not move `{` to the end of the line and
leaves `catch`/`finally` and some annotations where they are; human intervention is needed.

### Rejected OpenRewrite recipes

`org.openrewrite.java.format.AutoFormat` (bundles formatters that ignore the ruleset and
introduces star imports) and `TypecastParenPad` (turns `(T) x` into `(T)x`, violating
`WhitespaceAfter`) are not used.

## Running it

Three passes: OpenRewrite, the Groovy stage, then the OpenRewrite re-indent pass (the Groovy
`LineLength` wraps leave continuation lines mis-indented; the pass runs `TabsAndIndents` only, since
`Spaces` would turn the `{}` of empty blocks back into `{ }`). Review the result with `git diff`:

```sh
mvn org.openrewrite.maven:rewrite-maven-plugin:run@checkstyle-autofix-openrewrite \
    org.codehaus.gmavenplus:gmavenplus-plugin:execute@checkstyle-autofix \
    org.openrewrite.maven:rewrite-maven-plugin:run@checkstyle-reindent-openrewrite \
    -pl '!storm-shaded-deps' -Dcheckstyle.skip=true
```

`-Dcheckstyle.skip=true` is needed: `rewrite:run` forks the lifecycle up to
`process-test-classes`, which would otherwise run the `validate`-bound checkstyle check
and block the auto-fix on the very violations it is meant to fix. The Groovy stage runs
Checkstyle itself and is unaffected by the flag.

For a single module, replace the `-pl` argument (e.g. `-pl storm-client`); the Groovy
stage may need a second invocation of the same command if you want to be sure nothing
is left, since each run already repeats until nothing more can be fixed. To confirm
the result against the build's own rules afterwards:

```sh
mvn validate -pl '!storm-shaded-deps'
```

### Reviewing what it did

Both stages edit source files in place, so `git diff` shows exactly what changed.

### Upgrading Checkstyle

`checkstyle.version` (root `pom.xml`) is used by both the Checkstyle check and the
Groovy stage, so they can't disagree. If you change it, also update
`storm_checkstyle.xml` to match the new `google_checks.xml`: the Groovy fixers parse
Checkstyle's message text, and a reworded message makes that fixer skip the violation
(nothing breaks - Checkstyle still reports it).
