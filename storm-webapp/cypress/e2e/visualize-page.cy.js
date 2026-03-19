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

describe('Storm UI - Topology Visualization Page', () => {
  beforeEach(() => {
    cy.intercept('GET', '/api/v1/topology/*/visualization*').as('vizApi');
    cy.visit('/visualize.html?id=word-count-1-1234567890&sys=true');
    cy.wait('@vizApi');
  });

  it('loads the visualize bundle', () => {
    cy.get('script[src*="visualize.bundle.js"]').should('exist');
    cy.get('link[href*="visualize.bundle.css"]').should('exist');
  });

  it('has vis.js loaded as a global', () => {
    cy.window().then((win) => {
      expect(win.vis).to.exist;
      expect(win.vis.Network).to.exist;
      expect(win.vis.DataSet).to.exist;
    });
  });

  it('renders the network visualization container', () => {
    cy.get('#mynetwork').should('exist');
  });

  it('populates stream selector with discovered streams', () => {
    // The mock data has multiple streams: default, counts, __ack_init, __ack_ack
    cy.get('#available-streams li', { timeout: 5000 }).should('have.length.greaterThan', 0);
  });

  it('renders stream checkboxes that can be toggled', () => {
    cy.get('#available-streams input[type="checkbox"]', { timeout: 5000 })
      .should('have.length.greaterThan', 0);
    // System streams (__ack_*) should be unchecked by default
    // Stream panel is a slide-out, checkboxes may be offscreen
    cy.get('#available-streams input[type="checkbox"]').first().click({ force: true });
  });

  it('creates a vis.js network from the topology data', () => {
    cy.window().then((win) => {
      // visNS is set by visualization.js
      expect(win.visNS).to.exist;
      expect(win.visNS.nodes).to.exist;
      // Nodes should have been populated from the API response
      expect(win.visNS.nodes.length).to.be.greaterThan(0);
    });
  });
});

describe('Storm UI - Topology Visualization (Dark Mode)', () => {
  beforeEach(() => {
    cy.setCookie('stormTheme', 'dark');
    cy.intercept('GET', '/api/v1/topology/*/visualization*').as('vizApi');
    cy.visit('/visualize.html?id=word-count-1-1234567890&sys=true');
    cy.wait('@vizApi');
  });

  it('applies dark theme to the page', () => {
    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'dark');
  });

  it('gives the network container a dark background', () => {
    cy.get('#mynetwork').should(($el) => {
      const bg = window.getComputedStyle($el[0]).backgroundColor;
      expect(bg).to.equal('rgb(33, 37, 41)');
    });
  });

  it('renders nodes and streams in dark mode without errors', () => {
    cy.window().then((win) => {
      expect(win.visNS).to.exist;
      expect(win.visNS.nodes.length).to.be.greaterThan(0);
    });
    cy.get('#available-streams li', { timeout: 5000 })
      .should('have.length.greaterThan', 0);
  });

  it('switching to light mode gives network container a light background', () => {
    // Start in dark mode (from beforeEach), then switch to light
    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'dark');

    // Simulate theme toggle by setting the attribute directly
    cy.document().then((doc) => {
      doc.documentElement.setAttribute('data-bs-theme', 'light');
    });

    cy.get('#mynetwork').should(($el) => {
      const bg = window.getComputedStyle($el[0]).backgroundColor;
      // Must be white (light mode), NOT dark
      expect(bg).to.equal('rgb(255, 255, 255)');
    });
  });
});
