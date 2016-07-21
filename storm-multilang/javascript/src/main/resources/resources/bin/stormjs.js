#!/usr/bin/env node

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const PassThrough = require('stream').PassThrough;
const Console = require('console').Console;

// Storm using stdout to receive data from multilang components,
// so any console commands that write to stdout will cause
// trouble for storm, so we need to re-route any output
// from the console object to storm log commands for safety
const consoleStream = new PassThrough();
console = new Console(consoleStream);

if (process.argv.length < 3 || !process.argv[2])
    throw new Error('You must specify a js file that exports a valid storm component.');

let componentPath = process.argv[2];
if (componentPath.startsWith('.'))
    componentPath = process.cwd() + '/' + componentPath;

const Component = require(componentPath);

const componentInstance = new Component();
consoleStream.on('data', function(msg) { componentInstance.log(msg); });

componentInstance.run();