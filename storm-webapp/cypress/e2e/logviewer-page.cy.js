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

describe('Storm UI - Logviewer Pages', () => {
  it('logviewer landing page loads (no JS required)', () => {
    cy.visit('/logviewer.html');
    cy.get('body').should('exist');
    cy.contains('Storm').should('exist');
  });

  it('logviewer search page loads the main bundle', () => {
    cy.visit('/logviewer_search.html');
    cy.get('script[src*="main.bundle.js"]').should('exist');
    cy.get('link[href*="main.bundle.css"]').should('exist');
  });

  it('logviewer search page has jQuery and core plugins loaded', () => {
    cy.visit('/logviewer_search.html');
    cy.window().then((win) => {
      expect(win.$).to.exist;
      expect(win.jQuery).to.exist;
      expect(win.$.fn.DataTable).to.exist;
      expect(win.Mustache).to.exist;
    });
  });
});
