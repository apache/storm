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

describe('Storm UI - Flux Viewer Page', () => {
  beforeEach(() => {
    cy.visit('/flux.html');
  });

  it('loads the flux bundle', () => {
    cy.get('script[src*="flux.bundle.js"]').should('exist');
  });

  it('has cytoscape, dagre, esprima, and js-yaml loaded', () => {
    cy.window().then((win) => {
      expect(win.cytoscape).to.exist;
      expect(win.dagre).to.exist;
      expect(win.esprima).to.exist;
      expect(win.jsyaml).to.exist;
    });
  });

  it('renders the YAML input textarea', () => {
    cy.get('#taInput').should('exist');
  });

  it('renders the cytoscape graph container', () => {
    cy.get('#cy').should('exist');
  });

  it('can parse YAML and render a graph', () => {
    const yaml = [
      'spouts:',
      '  - id: "spout-1"',
      '    className: "org.example.TestSpout"',
      'bolts:',
      '  - id: "bolt-1"',
      '    className: "org.example.TestBolt"',
      'streams:',
      '  - from: "spout-1"',
      '    to: "bolt-1"',
      '    grouping:',
      '      type: SHUFFLE'
    ].join('\n');

    cy.get('#taInput').clear().type(yaml, { parseSpecialCharSequences: false });
    cy.contains('button', 'Refresh').click();
    // Cytoscape should have rendered nodes
    cy.get('#cy canvas').should('exist');
  });
});

describe('Storm UI - Flux Viewer (Dark Mode)', () => {
  it('respects dark theme from cookie', () => {
    cy.setCookie('stormTheme', 'dark');
    cy.visit('/flux.html');
    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'dark');
    cy.get('body').should(($body) => {
      const bg = window.getComputedStyle($body[0]).backgroundColor;
      expect(bg).to.equal('rgb(33, 37, 41)');
    });
  });
});
