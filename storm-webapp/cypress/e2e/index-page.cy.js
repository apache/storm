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

describe('Storm UI - Index Page', () => {
  beforeEach(() => {
    cy.visit('/');
  });

  it('loads the webpack main bundle without errors', () => {
    // The bundle script tag should be present
    cy.get('script[src*="main.bundle.js"]').should('exist');
    cy.get('link[href*="main.bundle.css"]').should('exist');
  });

  it('renders the page title from cluster config', () => {
    cy.title().should('contain', 'Storm');
  });

  it('renders the Cluster Summary section', () => {
    cy.get('#cluster-summary').should('exist');
    cy.contains('Cluster Summary').should('be.visible');
  });

  it('renders the Nimbus Summary table', () => {
    cy.get('#nimbus-summary').should('exist');
    cy.contains('Nimbus Summary').should('be.visible');
    // The nimbus host should appear in the table
    cy.get('#nimbus-summary').contains('nimbus-host-1');
  });

  it('renders the Topology Summary table with data', () => {
    cy.get('#topology-summary').should('exist');
    cy.contains('Topology Summary').should('be.visible');
    cy.get('#topology-summary').contains('word-count');
    cy.get('#topology-summary').contains('kafka-ingest');
  });

  it('renders the Supervisor Summary table', () => {
    cy.get('#supervisor-summary').should('exist');
    cy.contains('Supervisor Summary').should('be.visible');
    cy.get('#supervisor-summary').contains('supervisor-host-1');
  });

  it('renders the Nimbus Configuration table', () => {
    cy.get('#nimbus-configuration').should('exist');
    cy.contains('Nimbus Configuration').should('be.visible');
  });

  it('renders the Owner Summary section', () => {
    cy.get('#owner-summary').should('exist');
    cy.contains('Owner Summary').should('be.visible');
    cy.get('#owner-summary').contains('testuser');
  });

  it('has jQuery and key plugins loaded', () => {
    cy.window().then((win) => {
      expect(win.$).to.exist;
      expect(win.jQuery).to.exist;
      expect(win.$.fn.DataTable).to.exist;
      expect(win.Mustache).to.exist;
      expect(win.$.blockUI).to.exist;
    });
  });

  it('shows the page rendered timestamp', () => {
    cy.get('#page-rendered-at-timestamp').should('contain', 'Page rendered at:');
  });
});
