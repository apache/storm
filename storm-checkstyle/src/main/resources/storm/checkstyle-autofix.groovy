/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Auto-fixes storm_checkstyle.xml violations. Every fixer below reads the values it
 * needs (basicOffset, CustomImportOrder's rule string, etc.) straight out of the
 * ruleset XML itself via XmlSlurper - there is no formatter engine, preset style, or
 * hand-copied config anywhere in this script. If a value isn't declared in the
 * ruleset, the fixer that depends on it is skipped rather than guessing a default
 * borrowed from some other tool's convention.
 *
 * One fixer per checkstyle module below, each named and documented against the exact
 * module it closes:
 *
 *   Checkstyle module                          | Fixer
 *   --------------------------------------------|--------------------------------
 *   FileTabCharacter                             | fixFileTabCharacter
 *   NoWhitespaceBeforeCaseDefaultColon           | fixNoWhitespaceBeforeCaseDefaultColon
 *   NoWhitespaceBefore (LABELED_STAT token only) | fixNoWhitespaceBeforeLabeledStat
 *   CommentsIndentation                          | fixCommentsIndentation
 *   CustomImportOrder                            | fixCustomImportOrder
 *
 * Rules with no fixer here (LeftCurly/RightCurly brace placement, WhitespaceAround,
 * Indentation's statement re-nesting) need a real Java parser to fix correctly
 * without risking a semantic change or a wrong edit on ambiguous input - a plain
 * text/line pass isn't a safe way to move code across brace boundaries. Checkstyle
 * keeps checking them; nothing here silently claims to fix them.
 *
 * Run via gmavenplus-plugin's `execute` goal at the `validate` phase, before the
 * checkstyle check. Bound per-module (root pom pluginManagement, activated the same
 * way as Checkstyle) since project.compileSourceRoots / testCompileSourceRoots are
 * module-specific.
 */

import groovy.io.FileType
import groovy.xml.XmlSlurper

// ---------------------------------------------------------------------------------
// Load the active ruleset directly - storm.checkstyle.config picks the basename,
// same property maven-checkstyle-plugin's configLocation uses (root pom).
// ---------------------------------------------------------------------------------

String rulesetName = project.properties.getProperty('storm.checkstyle.config', 'storm_checkstyle')
String multiModuleDir = System.getProperty('maven.multiModuleProjectDirectory')
File rulesetFile = new File("${multiModuleDir}/storm-checkstyle/src/main/resources/storm/${rulesetName}.xml")
if (!rulesetFile.exists()) {
    println "[checkstyle-autofix] ${project.artifactId}: ruleset ${rulesetFile} not found, skipping."
    return
}

// Checkstyle rulesets always carry a <!DOCTYPE module PUBLIC ...> pointing at the
// public checkstyle DTD. XmlSlurper's default SAXParser rejects any DOCTYPE outright
// (disallow-doctype-decl=true, a blanket XXE hardening default), so it must be turned
// off here - but external entity/DTD fetching stays disabled so this doesn't reopen
// an XXE hole; the doctype is parsed, its external subset never is.
def xmlSlurper = new XmlSlurper()
xmlSlurper.setFeature('http://apache.org/xml/features/disallow-doctype-decl', false)
xmlSlurper.setFeature('http://xml.org/sax/features/external-general-entities', false)
xmlSlurper.setFeature('http://xml.org/sax/features/external-parameter-entities', false)
xmlSlurper.setFeature('http://apache.org/xml/features/nonvalidating/load-external-dtd', false)
def checker = xmlSlurper.parse(rulesetFile)
def treeWalker = checker.module.find { it.@name == 'TreeWalker' }

def findModule = { String name -> treeWalker.module.find { it.@name == name } }
def findModuleById = { String name, String id -> treeWalker.module.findAll { it.@name == name }.find { it.module.@id == id || it.@id.text() == id } }
def moduleProperty = { module, String propName, String defaultValue = null ->
    def prop = module?.property?.find { it.@name == propName }
    prop ? prop.@value.text() : defaultValue
}

def indentationModule = findModule('Indentation')
Integer basicOffset = (moduleProperty(indentationModule, 'basicOffset', '4')).toInteger()

def customImportOrderModule = findModule('CustomImportOrder')

boolean fileTabCharacterEnabled = findModule('FileTabCharacter') != null || checker.module.any { it.@name == 'FileTabCharacter' }

// ---------------------------------------------------------------------------------
// Shared line classification (comment vs. code vs. blank), reused by every fixer
// below that needs to tell them apart.
// ---------------------------------------------------------------------------------

def leadingWhitespace = { String line ->
    def m = (line =~ /^[ \t]*/)
    m.find()
    m.group()
}

// True if a CODE line's own (non-comment) content ends with a real statement
// terminator - ';', '{', or '}' - meaning whatever comes after it starts a new
// statement rather than continuing this one. A trailing "// ..." comment on the
// same line is stripped first so it can't hide the terminator.
def codeEndsWithTerminator = { String line ->
    int commentIdx = line.indexOf('//')
    String codeOnly = (commentIdx >= 0 ? line.substring(0, commentIdx) : line).trim()
    if (codeOnly.isEmpty()) {
        return false
    }
    def lastChar = codeOnly[-1]
    lastChar == ';' || lastChar == '{' || lastChar == '}'
}

// Line classification, tracked as a single top-to-bottom pass so a javadoc/block-comment
// continuation line (e.g. " * Provides a way...", which starts with "*" - not "//" or "/*")
// is correctly recognized as still being inside a comment rather than mistaken for code.
def classifyLines = { List<String> lines ->
    List<String> kinds = new ArrayList<>(lines.size()) // one of: BLANK, COMMENT, CODE
    boolean inBlock = false
    lines.each { String line ->
        String trimmed = line.trim()
        if (inBlock) {
            kinds << 'COMMENT'
            if (trimmed.contains('*/')) {
                inBlock = false
            }
        } else if (trimmed.isEmpty()) {
            kinds << 'BLANK'
        } else if (trimmed.startsWith('//')) {
            kinds << 'COMMENT'
        } else if (trimmed.startsWith('/*')) {
            kinds << 'COMMENT'
            if (!trimmed.contains('*/')) {
                inBlock = true
            }
        } else {
            kinds << 'CODE'
        }
    }
    kinds
}

/*
 * Checkstyle module: FileTabCharacter (property eachLine=true - every tab anywhere
 * in the file is flagged, not just leading indentation).
 * Fix: replace each tab with `basicOffset` spaces (Indentation module's own
 * basicOffset property - the ruleset's declared unit of indentation, not a
 * hardcoded width).
 */
def fixFileTabCharacter = { String line -> line.replace('\t', ' ' * basicOffset) }

/*
 * Checkstyle module: NoWhitespaceBeforeCaseDefaultColon
 * Flags: "case FOO :" and "default :" - a space between a switch label
 * (case value or the bare "default" keyword) and its colon.
 * Fix: drop that space. Restricted by the caller to CODE lines only, so a
 * comment containing the English words "case"/"default" ahead of an
 * unrelated colon elsewhere on the line can't be misread as a switch label.
 */
def fixNoWhitespaceBeforeCaseDefaultColon = { String line ->
    line
        .replaceAll(/\bdefault[ \t]+:/, 'default:')
        .replaceAll(/\bcase\b([^:{};]*)[ \t]+:/, 'case$1:')
}

/*
 * Checkstyle module: NoWhitespaceBefore, token LABELED_STAT only (the
 * module's other tokens - COMMA, SEMI, POST_INC, POST_DEC, DOT, METHOD_REF,
 * ELLIPSIS - are already handled correctly by any sane formatter and haven't
 * been observed to violate here, so they're not covered by this fixer).
 * Flags: "OUTERMOST_LOOP : for (...)" - a space between a statement label
 * and its colon.
 * Fix: drop that space. Matches only a line whose sole content before the
 * colon is a single identifier, so it can't be confused with a ternary
 * expression's colon or any other non-label use.
 */
def labeledStatSpacingPattern = ~/^([ \t]*[A-Za-z_$][A-Za-z0-9_$]*)[ \t]+(:.*)$/

def fixNoWhitespaceBeforeLabeledStat = { String line ->
    def m = labeledStatSpacingPattern.matcher(line)
    m.matches() ? (m.group(1) + m.group(2)) : line
}

/*
 * Checkstyle module: CommentsIndentation (tokens SINGLE_LINE_COMMENT,
 * BLOCK_COMMENT_BEGIN).
 * Flags: a standalone comment line (nothing but whitespace before "//" or
 * "/*") whose indentation doesn't match the line it logically attaches to:
 *   - normally, the next non-blank line;
 *   - but if that next line is only closing punctuation (e.g. a lone "}"),
 *     the comment is a trailing comment for the *previous* statement instead,
 *     and must match that statement's own starting indentation - skipping
 *     over any of that statement's wrapped continuation lines, which sit
 *     deeper than the statement's first line.
 * Fix: realign the comment (or, for a block comment, shift the whole block
 * as a unit to preserve javadoc "*" alignment) to whichever target applies.
 * Trailing end-of-line comments (code precedes "//" on the same line) are
 * untouched - CommentsIndentation doesn't check those either.
 * Mutates `result` in place; returns the number of lines changed.
 */
def fixCommentsIndentation = { List<String> result, List<String> kinds ->
    int linesFixed = 0
    int i = 0
    while (i < result.size()) {
        if (kinds[i] != 'COMMENT') {
            i++
            continue
        }
        // Maximal run of COMMENT/BLANK lines starting here, up to (not including)
        // the next CODE line or end of file.
        int runStart = i
        int j = i
        while (j < result.size() && kinds[j] != 'CODE') {
            j++
        }
        int runEnd = j - 1 // inclusive, last COMMENT/BLANK line of the run
        if (j >= result.size()) {
            i = j + 1
            continue
        }
        // Trim trailing BLANK lines from the run - only realign comment lines.
        while (runEnd >= runStart && kinds[runEnd] == 'BLANK') {
            runEnd--
        }
        if (runEnd < runStart) {
            i = j + 1
            continue
        }

        boolean nextIsCloserOnly = result[j].trim() ==~ /[)\}\];,]+/
        String targetIndent = null
        if (nextIsCloserOnly) {
            // Walk back to the nearest CODE line, then further back only through
            // that same statement's own wrapped-continuation lines - stopping as
            // soon as the line before it terminates its own, different statement
            // (ends with ';', '{', or '}'), which marks where the target
            // statement actually begins. Without this stop condition, a run of
            // unbroken code above (the common case - most methods have no blank
            // lines) would be walked indefinitely, picking up an unrelated,
            // unrelated-scope indentation from far earlier in the file.
            int p = runStart - 1
            while (p >= 0 && kinds[p] != 'CODE') {
                p--
            }
            if (p >= 0) {
                int stmtStart = p
                while (stmtStart - 1 >= 0 && kinds[stmtStart - 1] == 'CODE'
                        && !codeEndsWithTerminator(result[stmtStart - 1])) {
                    stmtStart--
                }
                targetIndent = leadingWhitespace(result[stmtStart])
                // If the statement found is itself a block opener ("... {"), the
                // comment sits *inside* that block (its only content, right before
                // the closing brace) rather than beside the opener - one level
                // deeper than the opener's own indentation.
                if (result[stmtStart].trim().endsWith('{')) {
                    targetIndent += ' ' * basicOffset
                }
            }
        }
        if (targetIndent == null) {
            targetIndent = leadingWhitespace(result[j])
        }

        // Single-line "//" comments have no internal structure, so each is set
        // directly to the target - a mid-run stray line that merely happens to
        // match its immediate neighbor (but not the eventual target) still gets
        // corrected. Block comments are shifted as a unit by one delta (computed
        // from the block's own opening line) to preserve javadoc "*" alignment.
        int x = runStart
        while (x <= runEnd) {
            if (kinds[x] == 'BLANK') {
                x++
                continue
            }
            String trimmedLine = result[x].trim()
            if (trimmedLine.startsWith('/*')) {
                int blockStart = x
                int blockEnd = x
                while (blockEnd < runEnd && !result[blockEnd].contains('*/')) {
                    blockEnd++
                }
                String blockCurIndent = leadingWhitespace(result[blockStart])
                int delta = targetIndent.length() - blockCurIndent.length()
                if (delta != 0) {
                    for (int y = blockStart; y <= blockEnd; y++) {
                        String curLine = result[y]
                        String curLead = leadingWhitespace(curLine)
                        String rest = curLine.substring(curLead.length())
                        int newLen = Math.max(0, curLead.length() + delta)
                        result[y] = (' ' * newLen) + rest
                        linesFixed++
                    }
                }
                x = blockEnd + 1
            } else {
                if (leadingWhitespace(result[x]) != targetIndent) {
                    result[x] = targetIndent + trimmedLine
                    linesFixed++
                }
                x++
            }
        }
        i = j + 1
    }
    linesFixed
}

/*
 * Checkstyle module: CustomImportOrder.
 * Reads customImportOrderRules (e.g. "STATIC###THIRD_PARTY_PACKAGE"),
 * sortImportsInGroupAlphabetically, and separateLineBetweenGroups directly from the
 * module's own properties - an empty/missing rule list disables this fixer entirely
 * rather than assuming a group order the ruleset never declared.
 * Fix: re-emit the whole leading import block (the contiguous run of "import "/
 * "import static " lines at the top of the file, before the first non-import,
 * non-comment, non-package line) grouped and ordered exactly per those properties,
 * with a blank line between groups only if separateLineBetweenGroups=true.
 * Only STATIC and THIRD_PARTY_PACKAGE groups are recognized (the two this ruleset
 * uses); a rule list naming any other group is left unfixed, since guessing that
 * group's package-prefix membership isn't something the ruleset spells out.
 * Returns the new list of lines, or the original if the block already matches or
 * the fixer doesn't apply.
 */
def fixCustomImportOrder = { List<String> lines ->
    if (customImportOrderModule == null) {
        return lines
    }
    String rule = moduleProperty(customImportOrderModule, 'customImportOrderRules', '')
    List<String> groupOrder = rule.split('###').findAll { it }
    if (groupOrder.isEmpty() || !groupOrder.every { it == 'STATIC' || it == 'THIRD_PARTY_PACKAGE' }) {
        return lines
    }
    boolean alphabetical = moduleProperty(customImportOrderModule, 'sortImportsInGroupAlphabetically', 'false') == 'true'
    boolean separateGroups = moduleProperty(customImportOrderModule, 'separateLineBetweenGroups', 'false') == 'true'

    int start = -1
    int end = -1
    for (int i = 0; i < lines.size(); i++) {
        String trimmed = lines[i].trim()
        if (trimmed.startsWith('import ')) {
            if (start < 0) {
                start = i
            }
            end = i
        } else if (start >= 0 && !trimmed.isEmpty()) {
            break
        }
    }
    if (start < 0) {
        return lines
    }

    List<String> importLines = lines[start..end].findAll { it.trim().startsWith('import ') }
    Map<String, List<String>> groups = ['STATIC': [], 'THIRD_PARTY_PACKAGE': []]
    importLines.each { String imp ->
        String key = imp.trim().startsWith('import static ') ? 'STATIC' : 'THIRD_PARTY_PACKAGE'
        groups[key] << imp.trim()
    }
    groupOrder.each { g -> if (alphabetical) { groups[g] = groups[g].sort() } }

    List<String> rebuilt = []
    groupOrder.eachWithIndex { g, idx ->
        if (groups[g].isEmpty()) {
            return
        }
        if (idx > 0 && separateGroups && !rebuilt.isEmpty()) {
            rebuilt << ''
        }
        rebuilt.addAll(groups[g])
    }

    if (rebuilt == lines[start..end]) {
        return lines
    }
    List<String> result = new ArrayList<>(lines.subList(0, start))
    result.addAll(rebuilt)
    result.addAll(lines.subList(end + 1, lines.size()))
    result
}

// Line-based fixers (module -> function), applied only to CODE lines.
def lineFixers = [
    NoWhitespaceBeforeCaseDefaultColon: fixNoWhitespaceBeforeCaseDefaultColon,
    NoWhitespaceBeforeLabeledStat     : fixNoWhitespaceBeforeLabeledStat,
] + (fileTabCharacterEnabled ? [FileTabCharacter: fixFileTabCharacter] : [:])

def sourceRoots = ((project.compileSourceRoots ?: []) + (project.testCompileSourceRoots ?: [])).unique()

int filesChanged = 0
Map<String, Integer> fixCounts = lineFixers.keySet().collectEntries { [(it): 0] }
fixCounts['CommentsIndentation'] = 0
fixCounts['CustomImportOrder'] = 0

sourceRoots.each { rootPath ->
    File rootDir = new File(rootPath)
    if (!rootDir.exists()) {
        return
    }
    rootDir.eachFileRecurse(FileType.FILES) { file ->
        if (!file.name.endsWith('.java')) {
            return
        }
        List<String> result = file.readLines()
        boolean changed = false

        List<String> reordered = fixCustomImportOrder(result)
        if (reordered != result) {
            result = reordered
            fixCounts['CustomImportOrder']++
            changed = true
        }

        // Classified once up front and reused by every fixer below. None of the
        // line-based edits (colon spacing, tabs) add/remove lines or change comment
        // boundaries, so the classification stays valid across all of them.
        List<String> kinds = classifyLines(result)

        lineFixers.each { moduleName, fixer ->
            for (int n = 0; n < result.size(); n++) {
                if (kinds[n] != 'CODE') {
                    continue
                }
                String original = result[n]
                String fixed = fixer(original)
                if (fixed != original) {
                    result[n] = fixed
                    fixCounts[moduleName]++
                    changed = true
                }
            }
        }

        int commentLinesFixed = fixCommentsIndentation(result, kinds)
        if (commentLinesFixed > 0) {
            fixCounts['CommentsIndentation'] += commentLinesFixed
            changed = true
        }

        if (changed) {
            file.text = result.join('\n') + '\n'
            filesChanged++
        }
    }
}

println "[checkstyle-autofix] ${project.artifactId}: fixed ${fixCounts} across ${filesChanged} file(s), ruleset=${rulesetName}.xml (basicOffset=${basicOffset})."
