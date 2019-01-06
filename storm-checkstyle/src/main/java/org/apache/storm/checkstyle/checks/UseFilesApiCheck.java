/*
 * Copyright 2019 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.checkstyle.checks;

import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.SeverityLevel;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;

/**
 * java.io file access methods do not set the FILE_SHARE_DELETE flag on Windows.
 * This causes Windows to prevent deletion of the files until the handle is released.
 * Since this behavior is unlike Linux, and would be a pain to try to avoid manually,
 * this check tries to catch uses of the file access object constructors in java.io that have this problem.
 * 
 * The same file handles can be opened via e.g. Files.newBufferedReader, which will set FILE_SHARE_DELETE by default.
 * The only exception is RandomAccessFile, which should be replaced with SeekableByteChannel from Files.newByteChannel.
 */
public class UseFilesApiCheck extends AbstractCheck {

    private final int[] tokens = new int [] {TokenTypes.LITERAL_NEW};
    private final List<String> bannedConstructors = Arrays.asList(
        FileInputStream.class.getSimpleName(),
        FileOutputStream.class.getSimpleName(),
        FileReader.class.getSimpleName(),
        FileWriter.class.getSimpleName(),
        RandomAccessFile.class.getSimpleName()
    );
    
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
        DetailAST arrayDecl = ast.findFirstToken(TokenTypes.ARRAY_DECLARATOR);
        if (arrayDecl != null) {
            //ensure this doesn't catch array declarations
            return;
        } 
        DetailAST parent = ast.getParent();
        DetailAST classIdent;
        if (parent.getType() == TokenTypes.METHOD_REF) {
            //Method reference, ident is a sibling of the node
            classIdent = parent.findFirstToken(TokenTypes.IDENT);
            if (classIdent == null) {
                //Can happen if the reference is to an array constructor (e.g. FileInputStream[]::new)
                return;
            }
        } else {
            //Regular constructor invocation, ident is a child of the node
            classIdent = ast.findFirstToken(TokenTypes.IDENT);
        }
        if (classIdent != null) { 
            String className = classIdent.getText();
            if (bannedConstructors.contains(className)) {
                log(classIdent, "Instantiating {0} directly. Please use java.nio.file.Files to open the file handle", className);
            } else if (File.class.getSimpleName().equals(className)) {
                log(classIdent, "Instantiating java.io.File. Please use java.nio.file.Path instead", className);
            }
        }
    }

}
