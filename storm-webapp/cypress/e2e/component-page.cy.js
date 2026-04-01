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

describe('Storm UI - Component Detail Page', () => {
  beforeEach(() => {
    cy.visit(
      '/component.html?id=split&topology_id=word-count-1-1234567890'
    );
  });

  it('loads the main bundle', () => {
    cy.get('script[src*="main.bundle.js"]').should('exist');
  });

  it('renders the component name', () => {
    cy.contains('split').should('exist');
  });

  it('renders executor stats', () => {
    cy.contains('Executors').should('exist');
  });

  it('has moment.js loaded for error time formatting', () => {
    cy.window().then((win) => {
      expect(win.moment).to.exist;
    });
  });

  it('formats large numbers with commas in bolt stats table', () => {
    cy.get('#bolt-stats-table').within(() => {
      cy.contains('td', '600,000,000,000').should('exist');
      cy.contains('td', '200,000,000,000').should('exist');
      cy.get('td').each(($td) => {
        expect($td.text()).not.to.match(/\de[+]\d/i);
      });
    });
  });

  it('formats large numbers with commas in output stats table', () => {
    cy.get('#bolt-output-stats-table').within(() => {
      cy.contains('td', '600,000,000,000').should('exist');
      cy.get('td').each(($td) => {
        expect($td.text()).not.to.match(/\de[+]\d/i);
      });
    });
  });

  it('formats large numbers with commas in input stats table', () => {
    cy.get('#bolt-input-stats-table').within(() => {
      cy.contains('td', '200,000,000,000').should('exist');
      cy.get('td').each(($td) => {
        expect($td.text()).not.to.match(/\de[+]\d/i);
      });
    });
  });
});
