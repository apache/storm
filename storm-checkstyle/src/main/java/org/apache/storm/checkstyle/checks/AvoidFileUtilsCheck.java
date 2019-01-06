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

package org.apache.storm.checkstyle.checks;

import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.FullIdent;
import com.puppycrawl.tools.checkstyle.api.SeverityLevel;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;
import java.util.Arrays;

/*
 * Certain file utility classes use the older file APIs that do not set FILE_SHARE_DELETE on Windows. We
 * should avoid these classes, and use java.nio.file classes to do file system operations instead.
 */
public class AvoidFileUtilsCheck extends AbstractCheck {

    private final int[] tokens = new int[]{TokenTypes.IMPORT};

    @Override
    public int[] getDefaultTokens() {
        return Arrays.copyOf(tokens, tokens.length);
    }

    @Override
    public int[] getAcceptableTokens() {
        return Arrays.copyOf(tokens, tokens.length);
    }

    @Override
    public int[] getRequiredTokens() {
        return Arrays.copyOf(tokens, tokens.length);
    }

    @Override
    public void init() {
        super.setSeverity(SeverityLevel.ERROR.name());
    }

    @Override
    public void visitToken(DetailAST ast) {
        FullIdent ident = FullIdent.createFullIdentBelow(ast);
        if (ident.getText().contains("org.apache.commons.io.FileUtils")) {
            log(ast, "Importing org.apache.commons.io.FileUtils. Please use java.nio.file instead.");
        } else if (ident.getText().contains("com.google.common.io.Files")) {
            log(ast, "Importing com.google.common.io.Files. Please use java.nio.file or com.google.common.io.MoreFiles instead.");
        } else if (ident.getText().contains("java.util.jar.JarFile")) {
            log(ast, "Importing java.util.jar.JarFile. Please use java.util.jar.JarInputStream instead.");
        } else if (ident.getText().contains("java.util.zip.ZipFile")) {
            log(ast, "Importing java.util.zip.ZipFile. Please use java.util.zip.ZipInputStream instead.");
        }
    }

}
