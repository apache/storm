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
 * A file is left alone when its leading comment is not an ASF license header, or when it carries
 * a copyright notice of anybody but the ASF (third-party works keep their own notice).
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

// '/* */' comment at the top of the file
String fixBlock(String text, String header) {
    def m = (text =~ /(?s)\A\s*\/\*.*?\*\/[ \t]*\n?/)
    if (!m.find() || !isAsfLicense(m.group())) {
        return text
    }
    return header + '\n' + text.substring(m.end()).replaceFirst(/^\n+/, '')
}

// '#' comment lines at the top, after an optional shebang
String fixHash(String text, String header) {
    String prefix = ''
    String rest = text
    if (rest.startsWith('#!')) {
        int nl = rest.indexOf('\n')
        prefix = rest.substring(0, nl + 1)
        rest = rest.substring(nl + 1).replaceFirst(/^\n+/, '')
    }
    def m = (rest =~ /\A(?:[ \t]*#[^\n]*(?:\n|\z))+/)
    if (!m.find() || !isAsfLicense(m.group())) {
        return text
    }
    String after = rest.substring(m.end()).replaceFirst(/^\n+/, '')
    return prefix + (prefix ? '\n' : '') + header + (after ? '\n' + after : '')
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
    if (!m.find() || !isAsfLicense(m.group())) {
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
