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

/*
 * Storm using stdout to receive data from multilang components,
 * so any console commands that write to stdout will cause
 * trouble for storm, so we need to re-route any output
 * from the console object to storm log commands for safety.
 */
const intercept = require('intercept-stdout');

let preInitMessages = '';
let initComplete = false;

const getLogInvalidMessages = (logLevel) => (
    (inputMsg) => {
        let msg = inputMsg;

        if (/^{\s*"pid"\s*:\s*\d+\s*}\nend\n$/.test(msg)) {
            initComplete = true;
            if (preInitMessages) {
                msg += preInitMessages;
                preInitMessages = undefined;
            }
        } else if (!/^\{.*\}\nend\n$/.test(msg)) {
            let m = { command: 'log', msg: msg.trim() };
            if (logLevel >= 0) m.level = logLevel;
            msg = `${JSON.stringify(m)}\nend\n`;
        }
        if (!initComplete) {
            /*
             * Storm multilang protocol expects first message to be a pid message, so anything written
             * to stdout or stderr before the pid message is cached and sent with the first message
             * after the PID has been processed.
             */
            preInitMessages += msg;
            msg = '';
        }

        return msg;
    }
);

// Sends stdout with log level INFO and stderr with log level ERROR
intercept(getLogInvalidMessages(2), getLogInvalidMessages(4));

if (process.argv.length < 3 || !process.argv[2])
    throw new Error('You must specify a js file that exports a valid storm component.');

let componentPath = process.argv[2];
if (componentPath.startsWith('.'))
    componentPath = process.cwd() + '/' + componentPath;

const Component = require(componentPath);
const componentInstance = new Component();

componentInstance.run();
