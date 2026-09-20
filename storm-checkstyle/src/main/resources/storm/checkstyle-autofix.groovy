/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Auto-fixes storm_checkstyle.xml violations.
 *
 * Script runs Checkstyle itself (in-process, same version and same ruleset the build's checkstyle check uses) and
 * reads back the exact violations - file, line, column, message.
 * Each violation is handed to a small fixer written for the one
 * Checkstyle module that reported it, which edits only the text that violation points at.
 * Because the fixers act on Checkstyle's own reports, they never touch code Checkstyle is
 * happy with, and there is no formatter with a style of its own to drift from the
 * ruleset. The audit/fix cycle repeats (a fix can unmask or move another violation)
 * until nothing more can be fixed. Values a fixer needs (Indentation's basicOffset,
 * LineLength's max, CustomImportOrder's rule string) are read straight from the ruleset
 * XML; a fixer whose parameters aren't declared there is skipped rather than guessing.
 *
 * Safety net, per file: a file that stops parsing after a fix is restored to its state
 * before the last round and left alone; a file that ends up with more violations than it
 * started with is restored to its original text.
 *
 * Only the rules OpenRewrite has no recipe for are fixed here (everything OpenRewrite can do,
 * fully or partly, runs in its stage, see rewrite.yml: the leftovers are not chased here).
 * Fixers (by Checkstyle module):
 *   Layout    OneStatementPerLine, LineLength (comments, argument/operator/call-chain wraps,
 *             long string literals)
 *   Wrapping  SeparatorWrap
 *   Ordering  OverloadMethodsDeclarationOrder, ConstructorsDeclarationGrouping
 *   Style     IllegalTokenText, AvoidEscapedUnicodeCharacters, TodoComment,
 *             single-line-comment-space (MatchXpath)
 *   Javadoc   JavadocLeadingAsteriskAlign, JavadocMissingLeadingAsterisk,
 *             JavadocContentLocation, JavadocParagraph, JavadocTagContinuationIndentation,
 *             RequireEmptyLineBeforeBlockTagGroup, AtclauseOrder, SummaryJavadoc
 *             (missing period, lowercase first word), InvalidJavadocPosition
 *
 * Deliberately not fixed - each needs a decision or knowledge a text edit can't have:
 *   Naming rules and AbbreviationAsWordInName (renames need cross-file symbol resolution),
 *   the MissingJavadoc checks, JavadocMethod and
 *   NonEmptyAtclauseDescription (need real prose), FallThrough and EmptyCatchBlock (a
 *   comment would only silence a possible bug), VariableDeclarationUsageDistance (moving a
 *   declaration can reorder side effects), OneTopLevelClass/OuterTypeFilename, LeftCurly on
 *   `case X: {`, TextBlockGoogleStyleFormatting. Checkstyle keeps checking all of these.
 *
 * Run on demand via gmavenplus-plugin's `execute` goal (execution id `checkstyle-autofix`,
 * bound to no phase, so a normal build never rewrites sources); see storm-checkstyle/README.md
 * for the command. Declared per-module (root pom pluginManagement) since
 * project.compileSourceRoots / testCompileSourceRoots are module-specific.
 */

import com.puppycrawl.tools.checkstyle.Checker
import com.puppycrawl.tools.checkstyle.ConfigurationLoader
import com.puppycrawl.tools.checkstyle.PropertiesExpander
import com.puppycrawl.tools.checkstyle.api.AuditEvent
import com.puppycrawl.tools.checkstyle.api.AuditListener
import groovy.io.FileType
import groovy.transform.CompileStatic
import groovy.xml.XmlSlurper

import java.util.regex.Matcher

// =================================================================================
// Src: a mutable source text plus the scanning helpers the fixers share. Everything is
// expressed in character offsets into the *current* text. `mask` classifies each char as
// code / comment / string-or-char-literal, so scans never mistake the contents of a
// string or comment for syntax.
// =================================================================================

@CompileStatic
class Src {
    
static final byte CODE = 0
    
static final byte COMMENT = 1
    
static final byte LITERAL = 2
    

private final StringBuilder sb
    
private byte[] maskCache

    Src(String text) {
        this.sb = new StringBuilder(text)
    }

    @Override
    String toString() {
        sb.toString()
    }

    int length() {
        sb.length()
    }

    char charAt(int i) {
        sb.charAt(i)
    }

    String slice(int from, int to) {
        sb.substring(from, to)
    }

    int indexOf(String s, int from) {
        sb.indexOf(s, from)
    }

    int lastIndexOf(String s, int from) {
        sb.lastIndexOf(s, from)
    }

    boolean has(int i, String s) {
        if (i < 0 || i + s.length() > sb.length()) {
            return false
        }
        for (int k = 0; k < s.length(); k++) {
            if (sb.charAt(i + k) != s.charAt(k)) {
                return false
            }
        }
        true
    }

    void replace(int from, int to, String s) {
        sb.replace(from, to, s)
        maskCache = null
    }

    void insert(int at, String s) {
        sb.insert(at, s)
        maskCache = null
    }

    void delete(int from, int to) {
        sb.delete(from, to)
        maskCache = null
    }

    // ---- lines ------------------------------------------------------------------

    int lineStart(int off) {
        off <= 0 ? 0 : sb.lastIndexOf('\n', off - 1) + 1
    }

    int lineEnd(int off) {
        int i = sb.indexOf('\n', off)
        i < 0 ? sb.length() : i
    }

    String line(int off) {
        slice(lineStart(off), lineEnd(off))
    }

    boolean isHorizontalWs(int i) {
        char c = sb.charAt(i)
        c == (char) ' ' || c == (char) '\t'
    }

    /** Index of the first non-blank char on the line that starts at ls (or the line end). */
    int firstNonWs(int ls) {
        int i = ls
        while (i < sb.length() && isHorizontalWs(i)) {
            i++
        }
        i
    }

    String indentOf(int off) {
        int ls = lineStart(off)
        slice(ls, firstNonWs(ls))
    }

    boolean isBlankLine(int off) {
        line(off).trim().isEmpty()
    }

    /** True if the line holds only comment text (and whitespace). */
    boolean isCommentOnlyLine(int off) {
        int ls = lineStart(off)
        int le = lineEnd(off)
        boolean seen = false
        for (int i = ls; i < le; i++) {
            if (Character.isWhitespace(sb.charAt(i))) {
                continue
            }
            if (mask()[i] != COMMENT) {
                return false
            }
            seen = true
        }
        seen
    }

    // ---- code/comment/literal classification ------------------------------------

    byte[] mask() {
        if (maskCache == null) {
            maskCache = computeMask(sb)
        }
        maskCache
    }

    boolean isCode(int i) {
        i >= 0 && i < sb.length() && mask()[i] == CODE
    }

    boolean isComment(int i) {
        i >= 0 && i < sb.length() && mask()[i] == COMMENT
    }

    boolean isLiteral(int i) {
        i >= 0 && i < sb.length() && mask()[i] == LITERAL
    }

    boolean isCodeChar(int i) {
        isCode(i) && !Character.isWhitespace(sb.charAt(i))
    }

    /** Last code (non-whitespace, non-comment, non-literal-interior is NOT excluded) char at or before i. */
    int prevCode(int i) {
        byte[] m = mask()
        for (int j = Math.min(i, sb.length() - 1); j >= 0; j--) {
            if (m[j] != COMMENT && !Character.isWhitespace(sb.charAt(j))) {
                return j
            }
        }
        -1
    }

    /** First code char at or after i (literals count: their opening quote is returned). */
    int nextCode(int i) {
        byte[] m = mask()
        for (int j = Math.max(i, 0); j < sb.length(); j++) {
            if (m[j] != COMMENT && !Character.isWhitespace(sb.charAt(j))) {
                return j
            }
        }
        -1
    }

    /** Index of the bracket closing the one at open, ignoring comments and literals; -1 if none. */
    int matchClose(int open) {
        char o = sb.charAt(open)
        char c = o == (char) '(' ? (char) ')' : (o == (char) '[' ? (char) ']' : (char) '}')
        byte[] m = mask()
        int depth = 0
        for (int i = open; i < sb.length(); i++) {
            if (m[i] != CODE) {
                continue
            }
            char ch = sb.charAt(i)
            if (ch == o) {
                depth++
            } else if (ch == c) {
                depth--
                if (depth == 0) {
                    return i
                }
            }
        }
        -1
    }

    /** Index of the bracket opening the one at close; -1 if none. */
    int matchOpen(int close) {
        char c = sb.charAt(close)
        char o = c == (char) ')' ? (char) '(' : (c == (char) ']' ? (char) '[' : (char) '{')
        byte[] m = mask()
        int depth = 0
        for (int i = close; i >= 0; i--) {
            if (m[i] != CODE) {
                continue
            }
            char ch = sb.charAt(i)
            if (ch == c) {
                depth++
            } else if (ch == o) {
                depth--
                if (depth == 0) {
                    return i
                }
            }
        }
        -1
    }

    /** The Java identifier/keyword starting at i ('' if none). */
    String wordAt(int i) {
        int j = i
        while (j < sb.length() && Character.isJavaIdentifierPart(sb.charAt(j))) {
            j++
        }
        slice(i, j)
    }

    static byte[] computeMask(CharSequence t) {
        int n = t.length()
        byte[] m = new byte[n]
        int i = 0
        while (i < n) {
            char c = t.charAt(i)
            char d = i + 1 < n ? t.charAt(i + 1) : (char) 0
            if (c == (char) '/' && d == (char) '/') {
                int j = i
                while (j < n && t.charAt(j) != (char) '\n') {
                    m[j++] = COMMENT
                }
                i = j
            } else if (c == (char) '/' && d == (char) '*') {
                int j = i + 2
                while (j < n && !(t.charAt(j) == (char) '*' && j + 1 < n && t.charAt(j + 1) == (char) '/')) {
                    j++
                }
                int end = Math.min(n, j + 2)
                for (int k = i; k < end; k++) {
                    m[k] = COMMENT
                }
                i = end
            } else if (c == (char) '"' && d == (char) '"' && i + 2 < n && t.charAt(i + 2) == (char) '"') {
                int j = i + 3
                while (j < n) {
                    char x = t.charAt(j)
                    if (x == (char) '\\') {
                        j += 2
                    } else if (x == (char) '"' && j + 2 < n + 0 && t.charAt(j + 1) == (char) '"'
                            && t.charAt(j + 2) == (char) '"') {
                        j += 3
                        break
                    } else {
                        j++
                    }
                }
                int end = Math.min(n, j)
                for (int k = i; k < end; k++) {
                    m[k] = LITERAL
                }
                i = end
            } else if (c == (char) '"' || c == (char) '\'') {
                int j = i + 1
                while (j < n && t.charAt(j) != c && t.charAt(j) != (char) '\n') {
                    j += t.charAt(j) == (char) '\\' ? 2 : 1
                }
                int end = Math.min(n, j < n && t.charAt(j) == c ? j + 1 : j)
                for (int k = i; k < end; k++) {
                    m[k] = LITERAL
                }
                i = end
            } else {
                i++
            }
        }
        m
    }
}

// =================================================================================
// Fixers. Every fixer has the signature
//     static int fix(Src s, int off, String msg, int[] origLineStarts)
// `off` is the offset Checkstyle reported (already translated to the current text; it is
// only ever handed to a fixer while everything before the previous edit is unchanged),
// `msg` its message. A fixer returns the lowest offset it modified, or NONE if it changed
// nothing. Violations are applied last-to-first, and any violation at or beyond an
// already-modified offset is deferred to the next audit round.
// =================================================================================

class Fixers {
    
static final int NONE = -1
    

/** Set from the ruleset: Indentation.basicOffset, LineLength.max. */
static int basicOffset = 4
    
static int maxLineLength = 100

    // ---- small helpers ----------------------------------------------------------

    static String tokenOf(String msg) {
        Matcher m = msg =~ /'([^']+)'/
        m.find() ? m.group(1) : null
    }

    static boolean hws(char c) {
        c == ' ' as char || c == '\t' as char
    }

    static String spaces(int n) {
        ' ' * Math.max(0, n)
    }

    static String nl(String indent) {
        '\n' + indent
    }

    static String oneIndent() {
        spaces(basicOffset)
    }

    /** Start of the run of comment lines that sit directly on top of the declaration at off. */
    static int leadingBlockStart(Src s, int off) {
        int ls = s.lineStart(off)
        while (ls > 0) {
            int pls = s.lineStart(ls - 1)
            if (s.isBlankLine(pls) || !s.isCommentOnlyLine(pls)) {
                break
            }
            ls = pls
        }
        ls
    }

    /** End (exclusive) of the member whose header starts at off: after its body's '}' or its ';'. */
    static int memberEnd(Src s, int off) {
        int n = s.length()
        int i = off
        while (i < n) {
            if (!s.isCode(i)) {
                i++
                continue
            }
            char c = s.charAt(i)
            if (c == '(' as char || c == '[' as char) {
                int e = s.matchClose(i)
                if (e < 0) {
                    return -1
                }
                i = e + 1
                continue
            }
            if (c == ';' as char) {
                return i + 1
            }
            if (c == '{' as char) {
                int e = s.matchClose(i)
                return e < 0 ? -1 : e + 1
            }
            i++
        }
        -1
    }

    /** End (exclusive) of the statement whose first token is at i; -1 if it can't be determined. */
    static int stmtEnd(Src s, int from) {
        int i = s.nextCode(from)
        if (i < 0) {
            return -1
        }
        char c = s.charAt(i)
        if (c == '{' as char) {
            int e = s.matchClose(i)
            return e < 0 ? -1 : e + 1
        }
        if (c == ';' as char) {
            return i + 1
        }
        String w = s.wordAt(i)
        switch (w) {
            case 'if':
            case 'for':
            case 'while':
                int p = s.nextCode(i + w.length())
                if (p < 0 || s.charAt(p) != '(' as char) {
                    return -1
                }
                int close = s.matchClose(p)
                if (close < 0) {
                    return -1
                }
                int body = stmtEnd(s, close + 1)
                if (w == 'if' && body >= 0) {
                    int nxt = s.nextCode(body)
                    if (nxt >= 0 && s.wordAt(nxt) == 'else') {
                        return stmtEnd(s, nxt + 4)
                    }
                }
                return body
            case 'do':
                int b = stmtEnd(s, i + 2)
                if (b < 0) {
                    return -1
                }
                int wh = s.nextCode(b)
                if (wh < 0 || s.wordAt(wh) != 'while') {
                    return -1
                }
                int wp = s.nextCode(wh + 5)
                int wc = wp < 0 ? -1 : s.matchClose(wp)
                int semi = wc < 0 ? -1 : s.nextCode(wc + 1)
                return semi >= 0 && s.charAt(semi) == ';' as char ? semi + 1 : -1
            case 'try':
                int tp = s.nextCode(i + 3)
                if (tp >= 0 && s.charAt(tp) == '(' as char) {
                    int rc = s.matchClose(tp)
                    tp = rc < 0 ? -1 : s.nextCode(rc + 1)
                }
                if (tp < 0 || s.charAt(tp) != '{' as char) {
                    return -1
                }
                int e = s.matchClose(tp) + 1
                while (e > 0) {
                    int nxt = s.nextCode(e)
                    String nw = nxt < 0 ? '' : s.wordAt(nxt)
                    if (nw == 'catch') {
                        int cp = s.nextCode(nxt + 5)
                        int cc = cp < 0 ? -1 : s.matchClose(cp)
                        int cb = cc < 0 ? -1 : s.nextCode(cc + 1)
                        if (cb < 0 || s.charAt(cb) != '{' as char) {
                            return -1
                        }
                        e = s.matchClose(cb) + 1
                    } else if (nw == 'finally') {
                        int fb = s.nextCode(nxt + 7)
                        if (fb < 0 || s.charAt(fb) != '{' as char) {
                            return -1
                        }
                        e = s.matchClose(fb) + 1
                    } else {
                        break
                    }
                }
                return e > 0 ? e : -1
            case 'switch':
            case 'synchronized':
                int sp = s.nextCode(i + w.length())
                int sc = sp < 0 || s.charAt(sp) != '(' as char ? -1 : s.matchClose(sp)
                int sb2 = sc < 0 ? -1 : s.nextCode(sc + 1)
                if (sb2 < 0 || s.charAt(sb2) != '{' as char) {
                    return -1
                }
                int se = s.matchClose(sb2)
                return se < 0 ? -1 : se + 1
            default:
                break
        }
        int j = i
        int n = s.length()
        while (j < n) {
            if (!s.isCode(j)) {
                j++
                continue
            }
            char ch = s.charAt(j)
            if (ch == '(' as char || ch == '[' as char || ch == '{' as char) {
                int e = s.matchClose(j)
                if (e < 0) {
                    return -1
                }
                j = e + 1
                continue
            }
            if (ch == ';' as char) {
                return j + 1
            }
            j++
        }
        -1
    }

    // ---- spacing ----------------------------------------------------------------

    static int commentSpace(Src s, int off, String msg, int[] ls) {
        if (msg.contains('must be followed by a whitespace') && s.has(off, '//')) {
            s.insert(off + 2, ' ')
            return off + 2
        }
        NONE
    }

    // ---- line structure ---------------------------------------------------------

    /** OneStatementPerLine reports the ';' of every statement after the first on a line. */
    static int oneStatementPerLine(Src s, int off, String msg, int[] ls) {
        if (!s.has(off, ';')) {
            return NONE
        }
        int lineStart = s.lineStart(off)
        int j = off - 1
        while (j >= lineStart) {
            if (!s.isCode(j)) {
                j--
                continue
            }
            char c = s.charAt(j)
            if (c == ')' as char || c == ']' as char) {
                int open = s.matchOpen(j)
                if (open < lineStart) {
                    return NONE
                }
                j = open - 1
                continue
            }
            if (c == ';' as char || c == '{' as char || c == '}' as char) {
                break
            }
            j--
        }
        if (j < lineStart) {
            return NONE
        }
        int k = j + 1
        while (k < off && s.isHorizontalWs(k)) {
            k++
        }
        if (k >= off || !s.isCode(k)) {
            return NONE
        }
        s.replace(j + 1, k, nl(s.indentOf(off)))
        j + 1
    }

    // ---- wrapping ---------------------------------------------------------------

    /** Operator/'.'/'::' left dangling at a line's end: move it to the start of the next line. */
    static int moveToNextLine(Src s, int off, String tok, boolean spaceAfter) {
        if (!s.has(off, tok)) {
            return NONE
        }
        int lineStart = s.lineStart(off)
        int le = s.lineEnd(off)
        String rest = s.slice(off + tok.length(), le).trim()
        String comment = null
        if (!rest.isEmpty()) {
            if (!rest.startsWith('//')) {
                return NONE
            }
            comment = rest
        }
        if (le >= s.length() - 1) {
            return NONE
        }
        int nls = le + 1
        int nfn = s.firstNonWs(nls)
        if (nfn >= s.lineEnd(nls) || s.isComment(nfn)) {
            return NONE
        }
        int j = off
        while (j > lineStart && s.isHorizontalWs(j - 1)) {
            j--
        }
        if (j == lineStart) {
            return NONE
        }
        s.insert(nfn, tok + (spaceAfter ? ' ' : ''))
        s.replace(j, le, comment != null ? ' ' + comment : '')
        j
    }

    /** ',' or '...' that starts a line: move it to the end of the previous line's code. */
    static int moveToPrevLine(Src s, int off, String tok) {
        if (!s.has(off, tok)) {
            return NONE
        }
        int lineStart = s.lineStart(off)
        if (lineStart == 0 || !s.slice(lineStart, off).trim().isEmpty()) {
            return NONE
        }
        int pc = s.prevCode(lineStart - 1)
        if (pc < 0) {
            return NONE
        }
        int after = off + tok.length()
        int k = after
        while (k < s.length() && s.isHorizontalWs(k)) {
            k++
        }
        int le = s.lineEnd(off)
        if (k >= le) {
            s.delete(lineStart, Math.min(s.length(), le + 1))
        } else {
            s.delete(off, k)
        }
        s.insert(pc + 1, tok)
        pc + 1
    }

    static int separatorWrap(Src s, int off, String msg, int[] ls) {
        String tok = tokenOf(msg)
        if (tok == null) {
            return NONE
        }
        if (msg.contains('should be on a new line')) {
            String line = s.line(off).trim()
            if (line.startsWith('import ') || line.startsWith('package ')) {
                return NONE
            }
            return moveToNextLine(s, off, tok, false)
        }
        if (msg.contains('should be on the previous line')) {
            return moveToPrevLine(s, off, tok)
        }
        NONE
    }

    // ---- ordering ---------------------------------------------------------------

    /** Moves the member at off (with its leading comments) to just after the member at targetLine. */
    static int moveMemberAfter(Src s, int off, int targetLine, int[] origLineStarts) {
        if (targetLine < 1 || targetLine > origLineStarts.length) {
            return NONE
        }
        int cs = leadingBlockStart(s, off)
        int ce = memberEnd(s, off)
        int t = s.firstNonWs(origLineStarts[targetLine - 1])
        int te = t >= cs ? -1 : memberEnd(s, t)
        if (ce < 0 || te < 0 || te > cs) {
            return NONE
        }
        String member = s.slice(cs, ce)
        int del = ce
        if (del < s.length() && s.charAt(del) == '\n' as char) {
            del++
        }
        s.delete(cs, del)
        if (cs > 0 && s.isBlankLine(s.lineStart(cs - 1)) && (cs >= s.length() || s.isBlankLine(cs)
                || s.line(cs).trim().startsWith('}'))) {
            int pb = s.lineStart(cs - 1)
            s.delete(pb, Math.min(s.length(), s.lineEnd(pb) + 1))
        }
        s.insert(te, '\n\n' + member)
        te
    }

    static int overloadOrder(Src s, int off, String msg, int[] ls) {
        Matcher m = msg =~ /line '(\d+)'/
        m.find() ? moveMemberAfter(s, off, m.group(1) as int, ls) : NONE
    }

    static int constructorGrouping(Src s, int off, String msg, int[] ls) {
        Matcher m = msg =~ /line '(\d+)'/
        m.find() ? moveMemberAfter(s, off, m.group(1) as int, ls) : NONE
    }
    

// ---- literals ---------------------------------------------------------------

private static final Map<Integer, String> ESCAPES = [
        8: '\\b', 9: '\\t', 10: '\\n', 12: '\\f', 13: '\\r', 32: ' ', 34: '\\"', 39: "\\'", 92: '\\\\']

    static int literalEnd(Src s, int off) {
        int e = off
        while (e < s.length() && s.isLiteral(e)) {
            e++
        }
        e
    }

    static int illegalTokenText(Src s, int off, String msg, int[] ls) {
        if (!s.isLiteral(off)) {
            return NONE
        }
        int end = literalEnd(s, off)
        String lit = s.slice(off, end)
        StringBuilder out = new StringBuilder()
        int i = 0
        while (i < lit.length()) {
            char c = lit.charAt(i)
            if (c != '\\' as char || i + 1 >= lit.length()) {
                out.append(c)
                i++
                continue
            }
            Matcher u = lit.substring(i) =~ /^\\u+([0-9a-fA-F]{4})/
            Matcher o = lit.substring(i) =~ /^\\(0(?:10|11|12|14|15|40|42|47)|134)(?![0-7])/
            if (u.find() && ESCAPES.containsKey(Integer.parseInt(u.group(1), 16))) {
                out.append(ESCAPES[Integer.parseInt(u.group(1), 16)])
                i += u.end()
            } else if (o.find()) {
                out.append(ESCAPES[Integer.parseInt(o.group(1), 8)])
                i += o.end()
            } else {
                out.append(c).append(lit.charAt(i + 1))
                i += 2
            }
        }
        if (out.toString() == lit) {
            return NONE
        }
        s.replace(off, end, out.toString())
        off
    }

    static int avoidEscapedUnicode(Src s, int off, String msg, int[] ls) {
        int start = off
        // the reported column may be the string's opening quote or the escape itself
        if (!s.isLiteral(off)) {
            return NONE
        }
        while (start > 0 && s.isLiteral(start - 1)) {
            start--
        }
        int end = literalEnd(s, off)
        String lit = s.slice(start, end)
        Matcher m = lit =~ /(?<!\\)((?:\\\\)*)\\u+([0-9a-fA-F]{4})(?:\\u+([0-9a-fA-F]{4}))?/
        StringBuffer out = new StringBuffer()
        boolean changed = false
        while (m.find()) {
            String pair = m.group(3) == null ? '' : m.group(3)
            String chars = new String(Character.toChars(Integer.parseInt(m.group(2), 16)))
            if (pair) {
                chars += new String(Character.toChars(Integer.parseInt(pair, 16)))
                if (chars.codePointCount(0, chars.length()) != 1) {
                    chars = new String(Character.toChars(Integer.parseInt(m.group(2), 16)))
                    m.appendReplacement(out, Matcher.quoteReplacement(m.group()))
                    continue
                }
            }
            int cp = chars.codePointAt(0)
            int type = Character.getType(cp)
            boolean printable = !(Character.isISOControl(cp) || Character.isWhitespace(cp) || Character.isSpaceChar(cp)
                    || type == Character.FORMAT || type == Character.UNASSIGNED || type == Character.PRIVATE_USE
                    || type == Character.SURROGATE || type == Character.LINE_SEPARATOR
                    || type == Character.PARAGRAPH_SEPARATOR || cp == 34 || cp == 39 || cp == 92)
            if (!printable || Character.charCount(cp) != chars.length()) {
                m.appendReplacement(out, Matcher.quoteReplacement(m.group()))
                continue
            }
            m.appendReplacement(out, Matcher.quoteReplacement(m.group(1) + chars))
            changed = true
        }
        m.appendTail(out)
        if (!changed) {
            return NONE
        }
        s.replace(start, end, out.toString())
        start
    }

    static int todoComment(Src s, int off, String msg, int[] ls) {
        int i = off
        while (i < s.length() && s.isHorizontalWs(i)) {
            i++
        }
        if (i + 4 > s.length() || !s.slice(i, i + 4).equalsIgnoreCase('todo') || !s.isComment(i)) {
            return NONE
        }
        int e = i + 4
        if (e < s.length() && s.charAt(e) == ':' as char) {
            e++
        }
        s.replace(i, e, 'TODO:')
        i
    }

    // ---- javadoc ----------------------------------------------------------------

    // [start, end) of the javadoc comment enclosing off, or null.
    static int[] javadocBounds(Src s, int off) {
        int start = s.lastIndexOf('/**', off)
        if (start < 0) {
            return null
        }
        int end = s.indexOf('*/', start + 3)
        if (end < 0 || end + 2 < off) {
            return null
        }
        [start, end + 2] as int[]
    }

    static int javadocAsteriskAlign(Src s, int off, String msg, int[] ls) {
        Matcher m = msg =~ /expected is (\d+)/
        if (!m.find()) {
            return NONE
        }
        int lineStart = s.lineStart(off)
        int fnw = s.firstNonWs(lineStart)
        if (fnw >= s.length() || s.charAt(fnw) != '*' as char) {
            return NONE
        }
        s.replace(lineStart, fnw, spaces((m.group(1) as int) - 1))
        lineStart
    }

    static int javadocMissingAsterisk(Src s, int off, String msg, int[] ls) {
        int[] b = javadocBounds(s, off)
        if (b == null) {
            return NONE
        }
        int col = b[0] - s.lineStart(b[0])
        int lineStart = s.lineStart(off)
        String t = s.line(off).trim()
        String fixed = t.startsWith('*/') ? spaces(col + 1) + t : (t.isEmpty() ? spaces(col + 1) + '*' : spaces(col + 1) + '* ' + t)
        s.replace(lineStart, s.lineEnd(off), fixed)
        lineStart
    }

    static int javadocContentLocation(Src s, int off, String msg, int[] ls) {
        if (!s.has(off, '/**')) {
            return NONE
        }
        int k = off + 3
        while (k < s.length() && s.isHorizontalWs(k)) {
            k++
        }
        if (k >= s.length() || s.charAt(k) == '\n' as char || s.has(k, '*/')) {
            return NONE
        }
        int col = off - s.lineStart(off)
        s.replace(off + 3, k, '\n' + spaces(col + 1) + '* ')
        off + 3
    }

    static int requireEmptyLineBeforeTags(Src s, int off, String msg, int[] ls) {
        int lineStart = s.lineStart(off)
        int fnw = s.firstNonWs(lineStart)
        if (fnw >= s.length() || s.charAt(fnw) != '*' as char || s.has(fnw, '*/')) {
            return NONE
        }
        s.insert(lineStart, s.slice(lineStart, fnw + 1) + '\n')
        lineStart
    }

    static int javadocParagraph(Src s, int off, String msg, int[] ls) {
        if (msg.contains('should be preceded with an empty line')) {
            int lineStart = s.lineStart(off)
            int fnw = s.firstNonWs(lineStart)
            if (fnw >= s.length() || s.charAt(fnw) != '*' as char || !s.slice(fnw + 1, off).trim().isEmpty()) {
                return NONE
            }
            s.insert(lineStart, s.slice(lineStart, fnw + 1) + '\n')
            return lineStart
        }
        if (msg.contains('immediately before the first word')) {
            if (!s.has(off, '<p>')) {
                return NONE
            }
            int k = off + 3
            while (k < s.length() && s.isHorizontalWs(k)) {
                k++
            }
            if (k < s.length() && s.charAt(k) == '\n' as char) {
                int nls = k + 1
                int nfn = s.firstNonWs(nls)
                if (nfn >= s.length() || s.charAt(nfn) != '*' as char || s.has(nfn, '*/')) {
                    return NONE
                }
                int text = nfn + 1
                while (text < s.length() && s.isHorizontalWs(text)) {
                    text++
                }
                if (text >= s.lineEnd(nls)) {
                    return NONE
                }
                s.delete(off + 3, text)
                return off + 3
            }
            if (k == off + 3) {
                return NONE
            }
            s.delete(off + 3, k)
            return off + 3
        }
        if (msg.contains('should be followed by <p>')) {
            int nls = s.lineEnd(off) + 1
            if (nls >= s.length()) {
                return NONE
            }
            int nfn = s.firstNonWs(nls)
            if (s.charAt(nfn) != '*' as char || s.has(nfn, '*/')) {
                return NONE
            }
            int text = nfn + 1
            while (text < s.length() && s.isHorizontalWs(text)) {
                text++
            }
            if (text >= s.lineEnd(nls) || s.charAt(text) == '@' as char || s.charAt(text) == '<' as char) {
                return NONE
            }
            s.insert(text, '<p>')
            return text
        }
        NONE
    }

    static int javadocTagContinuation(Src s, int off, String msg, int[] ls) {
        int lineStart = s.lineStart(off)
        Matcher m = s.line(off) =~ /^(\s*\*)\s*(\S.*)$/
        if (!m.matches()) {
            return NONE
        }
        s.replace(lineStart, s.lineEnd(off), m.group(1) + spaces(basicOffset + 1) + m.group(2))
        lineStart
    }

    static int atclauseOrder(Src s, int off, String msg, int[] ls) {
        int[] b = javadocBounds(s, off)
        if (b == null) {
            return NONE
        }
        List<String> lines = s.slice(b[0], b[1]).split('\n', -1).toList()
        if (lines.size() < 3 || lines.last().trim() != '*/') {
            return NONE
        }
        Map<String, Integer> rank = ['@param': 0, '@return': 1, '@throws': 2, '@deprecated': 3]
        int first = lines.findIndexOf {
            it.replaceFirst(/^\s*\*\s?/, '').startsWith('@')
        }
        if (first < 0) {
            return NONE
        }
        List<List<String>> blocks = []
        List<String> tags = []
        for (int i = first; i < lines.size() - 1; i++) {
            String content = lines[i].replaceFirst(/^\s*\*\s?/, '')
            if (content.startsWith('@')) {
                    blocks << []
                    tags << content.split(/[\s{]/)[0]
            }
                blocks.last() << lines[i]
        }
        List<Integer> slots = (0..<blocks.size()).findAll {
            rank.containsKey(tags[it])
        }
        List<Integer> ordered = slots.sort(false) {
            rank[tags[it]]
        }
        if (slots == ordered) {
            return NONE
        }
        List<List<String>> rebuilt = new ArrayList<>(blocks)
        for (int k = 0; k < slots.size(); k++) {
            rebuilt[slots[k]] = blocks[ordered[k]]
        }
        List<String> out = lines.subList(0, first) + rebuilt.flatten() + [lines.last()]
        s.replace(b[0], b[1], out.join('\n'))
        b[0]
    }

    static int summaryJavadoc(Src s, int off, String msg, int[] ls) {
        // Checkstyle reports these right after the opening slash-star-star: find the first text char
        while (off < s.length() && (Character.isWhitespace(s.charAt(off))
                || (s.charAt(off) == '*' as char && !s.has(off, '*/')))) {
            off++
        }
        if (msg.contains('Forbidden summary fragment')) {
            Matcher m = s.slice(off, Math.min(s.length(), off + 40)) =~ /^[a-z]+(?=[\s.,])/
            if (!m.find() || !s.isComment(off)) {
                return NONE
            }
            s.replace(off, off + 1, s.slice(off, off + 1).toUpperCase())
            return off
        }
        if (msg.contains('missing an ending period')) {
            int lineStart = s.lineStart(off)
            int lastEnd = -1
            int cur = lineStart
            boolean firstLine = true
            while (cur <= s.length()) {
                int le = s.lineEnd(cur)
                int textStart = firstLine ? off : s.firstNonWs(cur)
                if (!firstLine && textStart < le && s.charAt(textStart) == '*' as char && !s.has(textStart, '*/')) {
                    textStart++
                    while (textStart < le && s.isHorizontalWs(textStart)) {
                        textStart++
                    }
                }
                String content = s.slice(Math.min(textStart, le), le)
                int close = content.indexOf('*/')
                if (close >= 0) {
                    content = content.substring(0, close)
                }
                String t = content.trim()
                if (t.isEmpty() || (!firstLine && (t.startsWith('@') || t.startsWith('<p>')))) {
                    break
                }
                lastEnd = textStart + content.replaceAll(/\s+$/, '').length()
                if (close >= 0 || le >= s.length()) {
                    break
                }
                cur = le + 1
                firstLine = false
            }
            if (lastEnd < 0) {
                return NONE
            }
            char last = s.charAt(lastEnd - 1)
            if (last == ':' as char || last == '>' as char || last == '.' as char) {
                return NONE
            }
            s.insert(lastEnd, '.')
            return lastEnd
        }
        NONE
    }

    static int invalidJavadocPosition(Src s, int off, String msg, int[] ls) {
        if (s.has(off, '/**') && !s.has(off, '/**/')) {
            s.replace(off, off + 3, '/*')
            return off
        }
        NONE
    }

    // ---- indentation ------------------------------------------------------------

    // ---- line length ------------------------------------------------------------

    static int lineLength(Src s, int off, String msg, int[] ls) {
        int lineStart = s.lineStart(off)
        int le = s.lineEnd(off)
        String line = s.slice(lineStart, le)
        int max = maxLineLength
        if (line.length() <= max) {
            return NONE
        }
        int fnw = s.firstNonWs(lineStart)
        if (fnw >= le) {
            return NONE
        }
        if (s.isComment(fnw)) {
            return wrapComment(s, lineStart, fnw, le, max)
        }
        wrapCode(s, lineStart, fnw, le, max)
    }

    private static int wrapComment(Src s, int lineStart, int fnw, int le, int max) {
        String line = s.slice(lineStart, le)
        if (line =~ /(https?|ftp):\/\/|href\s*=/) {
            return NONE
        }
        String prefix
        String content
        if (s.has(fnw, '//')) {
            int t = fnw + 2
            while (t < le && s.isHorizontalWs(t)) {
                t++
            }
            prefix = s.slice(lineStart, fnw) + '// '
            content = s.slice(t, le).trim()
        } else if (s.charAt(fnw) == '*' as char && !s.has(fnw, '*/')) {
            int t = fnw + 1
            while (t < le && s.isHorizontalWs(t)) {
                t++
            }
            content = s.slice(t, le).trim()
            if (content.contains('*/') || content.startsWith('<pre') || content.contains('{@code')) {
                return NONE
            }
            String extra = ''
            // continuation of a block tag is indented one extra level; skip preformatted text
            int cur = lineStart
            while (cur > 0) {
                int pls = s.lineStart(cur - 1)
                String pl = s.line(pls).trim().replaceFirst(/^\/?\*+\s?/, '')
                if (pl.contains('<pre>') && !pl.contains('</pre>')) {
                    return NONE
                }
                if (pl.startsWith('@')) {
                    extra = spaces(basicOffset)
                    break
                }
                if (pl.isEmpty() || s.line(pls).trim().startsWith('/**') || s.line(pls).trim().startsWith('/*')) {
                    break
                }
                cur = pls
            }
            if (content.startsWith('@') || !extra.isEmpty()) {
                extra = spaces(basicOffset)
            }
            prefix = s.slice(lineStart, fnw + 1) + ' ' + (content.startsWith('@') ? '' : extra)
            if (content.startsWith('@')) {
                // wrapped tag lines continue indented
                prefix = s.slice(lineStart, fnw + 1) + ' ' + spaces(basicOffset)
                return wrapWords(s, lineStart, le, s.slice(lineStart, fnw + 1) + ' ', prefix, content, max)
            }
            return wrapWords(s, lineStart, le, s.slice(lineStart, fnw + 1) + ' ' + extra, prefix, content, max)
        } else {
            return NONE
        }
        wrapWords(s, lineStart, le, prefix, prefix, content, max)
    }

    private static int wrapWords(Src s, int lineStart, int le, String firstPrefix, String nextPrefix, String content, int max) {
        List<String> words = content.split(/\s+/).toList()
        List<String> out = []
        StringBuilder cur = new StringBuilder(firstPrefix)
        boolean empty = true
        for (String w : words) {
            int add = (empty ? 0 : 1) + w.length()
            if (!empty && cur.length() + add > max) {
                    out << cur.toString()
                cur = new StringBuilder(nextPrefix)
                empty = true
                add = w.length()
            }
            if (!empty) {
                cur.append(' ')
            }
            cur.append(w)
            empty = false
        }
            out << cur.toString()
        if (out.size() < 2 || out.any {
            it.length() > max && it.trim().split(/\s+/).size() > 2
        }) {
            if (out.size() < 2) {
                return NONE
            }
        }
        s.replace(lineStart, le, out.join('\n'))
        lineStart
    }

    private static int wrapCode(Src s, int lineStart, int fnw, int le, int max) {
        String head = s.slice(fnw, Math.min(le, fnw + 8))
        if (head.startsWith('import ') || head.startsWith('package ')) {
            return NONE
        }
        // candidate break offsets, all inside code
        int best = -1
        int depth = 0
        for (int i = fnw; i < le; i++) {
            if (!s.isCode(i)) {
                continue
            }
            char c = s.charAt(i)
            if (c == '(' as char || c == '[' as char) {
                depth++
            } else if (c == ')' as char || c == ']' as char) {
                depth--
            }
            int pos = -1
            if (c == ',' as char && depth > 0) {
                pos = i + 1
            } else if ((s.has(i, '&&') || s.has(i, '||')) && i > fnw && s.isHorizontalWs(i - 1)) {
                pos = i
            } else if (c == '+' as char && i > fnw && s.isHorizontalWs(i - 1) && i + 1 < le && s.isHorizontalWs(i + 1)) {
                pos = i
            } else if (c == '?' as char && i > fnw && s.isHorizontalWs(i - 1) && i + 1 < le && s.isHorizontalWs(i + 1)) {
                pos = i
            } else if (c == '.' as char && i > fnw && i + 1 < le && Character.isJavaIdentifierStart(s.charAt(i + 1))
                    && (s.charAt(i - 1) == ')' as char || Character.isJavaIdentifierPart(s.charAt(i - 1)))) {
                String after = s.slice(i + 1, le)
                if (after =~ /^\w+\s*\(/) {
                    pos = i
                }
            }
            if (pos > fnw && pos - lineStart <= max && (pos - lineStart) > (fnw - lineStart) + 8) {
                best = pos
            }
        }
        if (best < 0) {
            for (int i = fnw; i < le; i++) {
                if (s.isCode(i) && s.charAt(i) == '=' as char && s.isHorizontalWs(i - 1) && i + 1 < le && s.isHorizontalWs(i + 1)
                        && i + 1 - lineStart <= max && i + 1 > fnw + 8) {
                    best = i + 1
                }
            }
        }
        if (best < 0) {
            return splitString(s, lineStart, fnw, le, max)
        }
        int trimEnd = best
        while (trimEnd > lineStart && s.isHorizontalWs(trimEnd - 1)) {
            trimEnd--
        }
        int restStart = best
        while (restStart < le && s.isHorizontalWs(restStart)) {
            restStart++
        }
        if (restStart >= le) {
            return NONE
        }
        s.replace(trimEnd, restStart, nl(continuationIndent(s, lineStart, fnw)))
        trimEnd
    }

    /** Indent for a wrapped continuation: keep an existing continuation's indent, else statement indent + 2 levels. */
    static String continuationIndent(Src s, int lineStart, int fnw) {
        String indent = s.slice(lineStart, fnw)
        int p = lineStart > 0 ? s.prevCode(lineStart - 1) : -1
        boolean statementStart = p < 0 || s.charAt(p) == ';' as char || s.charAt(p) == '{' as char
                || s.charAt(p) == '}' as char
        statementStart ? indent + spaces(2 * basicOffset) : indent
    }

    private static int splitString(Src s, int lineStart, int fnw, int le, int max) {
        for (int i = fnw; i < le; i++) {
            if (!s.isLiteral(i) || s.charAt(i) != '"' as char || (i > 0 && s.isLiteral(i - 1))) {
                continue
            }
            int end = literalEnd(s, i)
            if (end - i < 6 || s.has(i, '"""') || end <= lineStart + max - 1 || end > le) {
                continue
            }
            int prev = s.prevCode(i - 1)
            int next = s.nextCode(end)
            if (prev < 0 || next < 0) {
                return NONE
            }
            char pc = s.charAt(prev)
            String pw = Character.isJavaIdentifierPart(pc) ? s.slice(Math.max(0, prev - 5), prev + 1) : ''
            char nc = s.charAt(next)
            boolean okPrev = pc in ['(', ',', '=', '+', '?', ':', '{'] as char[] || pw.endsWith('return') || pw.endsWith('case')
            boolean okNext = nc in [',', ')', ';', '+', '?', ':', '}'] as char[]
            if (!okPrev || !okNext) {
                return NONE
            }
            // safe split points: boundaries that don't cut an escape sequence
            List<Integer> safe = []
            int k = i + 1
            while (k < end - 1) {
                if (s.charAt(k) == '\\' as char) {
                    Matcher u = s.slice(k, end) =~ /^\\u+[0-9a-fA-F]{4}/
                    k += u.find() ? u.end() : 2
                } else {
                    k++
                }
                    safe << k
            }
            int limit = lineStart + max - 2
            int cut = -1
            for (int cand : safe) {
                if (cand <= limit && cand > i + 8 && s.charAt(cand - 1) == ' ' as char) {
                    cut = cand
                }
            }
            if (cut < 0) {
                for (int cand : safe) {
                    if (cand <= limit && cand > i + 8) {
                        cut = cand
                    }
                }
            }
            if (cut < 0 || cut >= end - 1) {
                return NONE
            }
            s.replace(cut, cut, '"' + nl(continuationIndent(s, lineStart, fnw)) + '+ "')
            return cut
        }
        NONE
    }
}

// =================================================================================
// Script body
// =================================================================================

// ---------------------------------------------------------------------------------
// Load the ruleset directly - the same file maven-checkstyle-plugin's configLocation uses (root pom).
// ---------------------------------------------------------------------------------

String multiModuleDir = System.getProperty('maven.multiModuleProjectDirectory')
File rulesetFile = new File("${multiModuleDir}/storm-checkstyle/src/main/resources/storm/storm_checkstyle.xml")
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
def treeWalker = checker.module.find {
    it.@name == 'TreeWalker'
}

def findModule = { String name ->
    treeWalker.module.find {
        it.@name == name
    }
}
def moduleProperty = { module, String propName, String defaultValue = null ->
    def prop = module?.property?.find {
        it.@name == propName
    }
    prop ? prop.@value.text() : defaultValue
}

Fixers.basicOffset = moduleProperty(findModule('Indentation'), 'basicOffset', '4').toInteger()
Fixers.maxLineLength = moduleProperty(checker.module.find {
    it.@name == 'LineLength'
}, 'max', '100').toInteger()


def sourceRoots = ((project.compileSourceRoots ?: []) + (project.testCompileSourceRoots ?: [])).unique()
// Honour the same <excludes> the module gives maven-checkstyle-plugin (e.g. thrift-generated
// code): those files aren't checked, so they must not be rewritten - or even audited, which
// for the huge generated classes is very slow.
def checkstylePlugin = project.build?.plugins?.find {
    it.artifactId == 'maven-checkstyle-plugin'
}
List<java.nio.file.PathMatcher> excludeMatchers = (checkstylePlugin?.configuration?.getChild('excludes')?.value ?: '')
        .split(',').collect {
    it.trim()
}.findAll {
    it
}
        .collect {
            java.nio.file.FileSystems.default.getPathMatcher("glob:/${it}".toString())
        }
List<File> javaFiles = []
sourceRoots.each { rootPath ->
    File rootDir = new File(rootPath)
    if (rootDir.exists()) {
        rootDir.eachFileRecurse(FileType.FILES) { file ->
            String relative = '/' + rootDir.toPath().relativize(file.toPath()).toString().replace(File.separator, '/')
            if (file.name.endsWith('.java') && !excludeMatchers.any {
                it.matches(java.nio.file.Paths.get(relative))
            }) {
                    javaFiles << file
            }
        }
    }
}

// Stage 2: audit with Checkstyle, fix what each reported violation points at, repeat.
Map<String, Closure> fixers = [
        OneStatementPerLine               : Fixers.&oneStatementPerLine,
        LineLength                        : Fixers.&lineLength,
        MatchXpath                        : Fixers.&commentSpace,
        SeparatorWrap                     : Fixers.&separatorWrap,
        OverloadMethodsDeclarationOrder   : Fixers.&overloadOrder,
        ConstructorsDeclarationGrouping   : Fixers.&constructorGrouping,
        IllegalTokenText                  : Fixers.&illegalTokenText,
        AvoidEscapedUnicodeCharacters     : Fixers.&avoidEscapedUnicode,
        TodoComment                       : Fixers.&todoComment,
        JavadocLeadingAsteriskAlign       : Fixers.&javadocAsteriskAlign,
        JavadocMissingLeadingAsterisk     : Fixers.&javadocMissingAsterisk,
        JavadocContentLocation            : Fixers.&javadocContentLocation,
        JavadocParagraph                  : Fixers.&javadocParagraph,
        JavadocTagContinuationIndentation : Fixers.&javadocTagContinuation,
        RequireEmptyLineBeforeBlockTagGroup: Fixers.&requireEmptyLineBeforeTags,
        AtclauseOrder                     : Fixers.&atclauseOrder,
        SummaryJavadoc                    : Fixers.&summaryJavadoc,
        InvalidJavadocPosition            : Fixers.&invalidJavadocPosition,
]

final int MAX_ROUNDS = 25

Map<String, List<Map>> violations = [:]
Set<String> unparsable = [] as Set

Properties checkstyleProps = new Properties()
checkstyleProps.setProperty('org.checkstyle.google.severity', 'error')
def config = ConfigurationLoader.loadConfiguration(rulesetFile.absolutePath, new PropertiesExpander(checkstyleProps))
def auditor = new Checker()
auditor.setModuleClassLoader(Checker.classLoader)
// columns then equal char offsets + 1 even if a tab slips through
auditor.setTabWidth(1)
auditor.configure(config)
auditor.addListener([
        auditStarted : { AuditEvent e ->
        },
        auditFinished: { AuditEvent e ->
        },
        fileStarted  : { AuditEvent e ->
        },
        fileFinished : { AuditEvent e ->
        },
        addError     : { AuditEvent e ->
            String key = e.sourceName.substring(e.sourceName.lastIndexOf('.') + 1).replaceFirst(/Check$/, '')
            violations.get(e.fileName, []) << [line: e.line, col: e.column, msg: e.message, check: key]
        },
        addException : { AuditEvent e, Throwable t ->
            unparsable << e.fileName
        },
] as AuditListener)

// One file per call: Checkstyle throws out of process() on a file that doesn't parse.
def audit = { List<File> files ->
    violations.clear()
    unparsable.clear()
    files.each { File f ->
        try {
            auditor.process([f])
        } catch (Exception ex) {
                unparsable << f.absolutePath
        }
    }
}

Map<File, String> originalText = [:]
Map<File, String> beforeRound = [:]
Map<File, Integer> initialCount = [:]
Map<File, Integer> latestCount = [:]
Map<String, Integer> appliedByCheck = [:].withDefault {
    0
}
Set<File> frozen = [] as Set
Map<String, String> fixerErrors = [:]

// skip CRLF files: the fixers write '\n'
List<File> pending = javaFiles.findAll { File f ->
    !f.getText('UTF-8').contains('\r')
}
int round = 0
while (!pending.isEmpty() && round < MAX_ROUNDS) {
    round++
    pending.each { File f ->
        originalText.putIfAbsent(f, f.getText('UTF-8'))
    }
    audit(pending)
    List<File> next = []
    pending.each { File f ->
        List<Map> vs = violations[f.absolutePath] ?: []
        latestCount[f] = vs.size()
        if (!initialCount.containsKey(f)) {
            initialCount[f] = vs.size()
        }
        if (unparsable.contains(f.absolutePath)) {
            if (beforeRound.containsKey(f)) {
                f.setText(beforeRound[f], 'UTF-8')
                    frozen << f
                println "[checkstyle-autofix] ${f}: a fix left the file unparsable; reverted the last round."
            }
            return
        }
        String text = f.getText('UTF-8')
        List<Map> fixable = vs.findAll {
            fixers.containsKey(it.check)
        }
        if (fixable.isEmpty()) {
            return
        }
        int[] lineStarts = lineStartsOf(text)
        Src src = new Src(text)
        List<Map> work = fixable.collect { Map v ->
            v + [off: v.line >= 1 && v.line <= lineStarts.length ? lineStarts[v.line - 1] + Math.max(0, v.col - 1) : -1]
        }.findAll {
            it.off >= 0
        }
        // last-to-first; on ties handle the token's trailing side first
        work.sort { a, b ->
            b.off <=> a.off
        }
        int lowest = Integer.MAX_VALUE
        work.each { Map v ->
            if (v.off >= lowest) {
                return
            }
            int touched
            try {
                touched = (fixers[v.check].call(src, v.off, v.msg, lineStarts) as int)
            } catch (Exception ex) {
                fixerErrors[v.check] = "${ex.class.simpleName}: ${ex.message}".toString()
                touched = Fixers.NONE
            }
            if (touched >= 0) {
                lowest = Math.min(lowest, touched)
                appliedByCheck[v.check]++
            }
        }
        String updated = src.toString()
        if (updated != text) {
            beforeRound[f] = text
            f.setText(updated, 'UTF-8')
                next << f
        }
    }
    pending = next.findAll {
        !frozen.contains(it)
    }
}
if (!pending.isEmpty()) {
    audit(pending)
    pending.each { File f ->
        latestCount[f] = (violations[f.absolutePath] ?: []).size()
    }
}

// A file that ended with more violations than it started with is put back as it was.
int filesChanged = 0
originalText.each { File f, String original ->
    if (f.getText('UTF-8') == original) {
        return
    }
    if (latestCount[f] != null && initialCount[f] != null && latestCount[f] > initialCount[f]) {
        f.setText(original, 'UTF-8')
        println "[checkstyle-autofix] ${f}: fixing made it worse (${initialCount[f]} -> ${latestCount[f]} violations); restored."
    } else {
        filesChanged++
    }
}
auditor.destroy()

println "[checkstyle-autofix] ${project.artifactId}: " +
        "fixes applied ${appliedByCheck.sort()} over ${round} round(s), ${filesChanged} file(s) changed, " +
        "ruleset=storm_checkstyle.xml." + (fixerErrors ? " Fixer errors (bug in the fixer, violation left as is): ${fixerErrors}" : '')

static int[] lineStartsOf(String text) {
    List<Integer> starts = [0]
    int i = text.indexOf('\n')
    while (i >= 0) {
            starts << i + 1
        i = text.indexOf('\n', i + 1)
    }
    starts as int[]
}
