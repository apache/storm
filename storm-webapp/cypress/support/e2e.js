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

// Collect JS errors from the app for debugging
const jsErrors = [];

Cypress.on('uncaught:exception', (err, runnable) => {
  jsErrors.push(err.message);
  // Let the error propagate so tests fail visibly
  return false;
});

// Make collected errors available in tests
Cypress.Commands.add('getJsErrors', () => {
  return cy.wrap(jsErrors);
});

beforeEach(() => {
  jsErrors.length = 0;
});
