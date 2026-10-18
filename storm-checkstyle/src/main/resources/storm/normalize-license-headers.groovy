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
 * Rewrites the leading license comment of the files of a module (Java, Groovy, XML, Markdown,
 * YAML, properties, Python and shell) to the exact template of
 * https://www.apache.org/legal/src-headers.html#headers.
 *
 * A file is left alone when its leading comment is not an ASF license header, when it carries
 * a copyright notice of anybody but the ASF (third-party works keep their own notice), or when its
 * header already has the words of the template, whatever the line wrapping, the case (the older
 * 'to You' form) or the Javadoc '<p>' an IDE formatter adds: rewrapping such a header would touch
 * most files of the code base and pollute git blame for no legal gain. So only headers whose
 * wording is wrong (typos, missing words) are rewritten.
 *
 * Only the license lines are replaced: a block or XML comment with anything besides the license is
 * left alone, and the '#' lines around the license (e.g. a '-*- coding -*-' line) are kept.
 *
 * Run on demand via gmavenplus-plugin's `execute` goal (execution id `normalize-license-headers`,
 * bound to no phase); see storm-checkstyle/README.md. With -Dlicense.check=true nothing is
 * written and the build fails if a file would change.
 */

final List<String> TEXT = '''\
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.'''.readLines()

boolean checkOnly = Boolean.getBoolean('license.check')

// the words of a comment, without comment markers and line wrapping
String words(String comment) {
    return comment.readLines()
        .collect { it.replaceAll(/\*\/|-->|<p\s*\/?>/, '').replaceFirst(/^\s*(\/\*+|\*|#|<!--|~)?/, '') }
        .join(' ').split(/\s+/).findAll().join(' ')
}

templateWords = words(TEXT.join('\n')).toLowerCase()

blockHeader = '/*\n' + TEXT.collect { it.isEmpty() ? ' *' : ' * ' + it }.join('\n') + '\n */\n'
hashHeader = TEXT.collect { it.isEmpty() ? '#' : '# ' + it }.join('\n') + '\n'
xmlHeader = '<!--\n' + TEXT.collect { it.isEmpty() ? '' : ' ' + it }.join('\n') + '\n-->\n'

boolean isAsfLicense(String comment) {
    if (!comment.contains('Apache Software Foundation') || !(comment.contains('Licensed to') || comment.contains('Licensed under'))) {
        return false
    }
    // a copyright line of somebody else: a third-party work
    return !comment.readLines().any { it.contains('Copyright') && !it.contains('Apache Software Foundation') }
}

// a comment holding the license and nothing else, and not already worded as the template
boolean needsRewrite(String comment) {
    String w = words(comment)
    return isAsfLicense(comment) && w.startsWith('Licensed ') && w.endsWith('under the License.') && w.toLowerCase() != templateWords
}

// '/* */' comment at the top of the file
String fixBlock(String text, String header) {
    def m = (text =~ /(?s)\A\s*\/\*.*?\*\/[ \t]*\n?/)
    if (!m.find() || !needsRewrite(m.group())) {
        return text
    }
    return header + '\n' + text.substring(m.end()).replaceFirst(/^\n+/, '')
}

// '#' comment lines at the top, after an optional shebang: only the license lines are replaced
String fixHash(String text, String header) {
    String prefix = ''
    String rest = text
    if (rest.startsWith('#!')) {
        int nl = rest.indexOf('\n')
        prefix = rest.substring(0, nl + 1)
        rest = rest.substring(nl + 1)
    }
    def m = (rest =~ /\A(\s*)((?:[ \t]*#[^\n]*(?:\n|\z))+)/)
    if (!m.find()) {
        return text
    }
    // the license runs from the 'Licensed ...' line to the '... under the License.' line
    List<String> lines = m.group(2).readLines()
    int start = lines.findIndexOf { it =~ /Licensed (to|under)/ }
    int end = start < 0 ? -1 : lines.findIndexOf(start) { it.contains('under the License.') }
    if (end < 0 || !needsRewrite(lines[start..end].join('\n'))) {
        return text
    }
    List<String> run = lines.subList(0, start) + header.readLines() + lines.subList(end + 1, lines.size())
    return prefix + m.group(1) + run.join('\n') + (m.group(2).endsWith('\n') ? '\n' : '') + rest.substring(m.end())
}

// '<!-- -->' comment at the top, after an optional '<?xml ...?>' declaration
String fixXml(String text, String header) {
    String prefix = ''
    String rest = text
    def decl = (rest =~ /\A<\?xml[^>]*\?>[ \t]*\n/)
    if (decl.find()) {
        prefix = decl.group()
        rest = rest.substring(decl.end()).replaceFirst(/^\n+/, '')
    }
    def m = (rest =~ /(?s)\A\s*<!--.*?-->[ \t]*\n?/)
    if (!m.find() || !needsRewrite(m.group())) {
        return text
    }
    String after = rest.substring(m.end()).replaceFirst(/^\n+/, '')
    return prefix + header + (after ? '\n' + after : '')
}

Closure fixerFor(String name) {
    if (name.endsWith('.java') || name.endsWith('.groovy')) {
        return { String t -> fixBlock(t, blockHeader) }
    }
    if (name.endsWith('.xml') || name.endsWith('.md')) {
        return { String t -> fixXml(t, xmlHeader) }
    }
    if (['.yaml', '.yml', '.properties', '.py', '.sh'].any { name.endsWith(it) }) {
        return { String t -> fixHash(t, hashHeader) }
    }
    return null
}

Set<String> skippedDirs = ['target', 'node_modules', 'generated', '.git'] as Set
List<String> changed = []
File base = project.basedir
base.eachFileRecurse(groovy.io.FileType.FILES) { File f ->
    String rel = base.toPath().relativize(f.toPath()).toString()
    if (rel.split('/').any { skippedDirs.contains(it) }) {
        return
    }
    Closure fix = fixerFor(f.name)
    if (fix == null) {
        return
    }
    String text = f.getText('UTF-8')
    String fixed = fix(text)
    if (fixed != text) {
        changed << rel
        if (!checkOnly) {
            f.setText(fixed, 'UTF-8')
        }
    }
}

println "[normalize-license-headers] ${project.artifactId}: ${changed.size()} file(s) ${checkOnly ? 'need a header fix' : 'rewritten'}"
if (checkOnly && !changed.isEmpty()) {
    changed.each { println "  ${it}" }
    throw new IllegalStateException('License headers differ from the ASF template')
}
