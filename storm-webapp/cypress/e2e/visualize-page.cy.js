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
    cy.visit('/visualize.html?id=word-count-1-1234567890&sys=false');
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

  it('fetches topology visualization data from the API', () => {
    // Intercept the visualization API call to verify it was made
    cy.intercept('GET', '/api/v1/topology/*/visualization*').as('vizApi');
    cy.visit('/visualize.html?id=word-count-1-1234567890&sys=false');
    cy.wait('@vizApi').its('response.statusCode').should('be.oneOf', [200, 304]);
  });

  it('shows the stream selector panel', () => {
    cy.get('#available-streams').should('exist');
  });
});
