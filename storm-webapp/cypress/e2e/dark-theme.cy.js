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

describe('Storm UI - Dark Theme', () => {

  it('applies light theme when cookie is set to light', () => {
    cy.setCookie('stormTheme', 'light');
    cy.visit('/');
    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'light');
  });

  it('applies dark theme when cookie is set to dark', () => {
    cy.setCookie('stormTheme', 'dark');
    cy.visit('/');
    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'dark');
  });

  it('dark theme gives body a dark background color', () => {
    cy.setCookie('stormTheme', 'dark');
    cy.visit('/');
    cy.get('body').should(($body) => {
      const bg = window.getComputedStyle($body[0]).backgroundColor;
      // Bootstrap 5.3 dark theme uses #212529 = rgb(33, 37, 41)
      expect(bg).to.equal('rgb(33, 37, 41)');
    });
  });

  it('light theme gives body a white/light background color', () => {
    cy.setCookie('stormTheme', 'light');
    cy.visit('/');
    cy.get('body').should(($body) => {
      const bg = window.getComputedStyle($body[0]).backgroundColor;
      // Bootstrap 5.3 light theme uses #fff = rgb(255, 255, 255)
      expect(bg).to.equal('rgb(255, 255, 255)');
    });
  });

  it('toggle button switches theme from light to dark', () => {
    cy.setCookie('stormTheme', 'light');
    cy.visit('/');
    cy.get('#theme-toggle-btn', { timeout: 5000 })
      .should('have.value', 'Dark Mode');

    cy.get('#theme-toggle-btn').click();

    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'dark');
    cy.get('#theme-toggle-btn').should('have.value', 'Light Mode');
  });

  it('toggle button switches theme from dark to light', () => {
    cy.setCookie('stormTheme', 'dark');
    cy.visit('/');
    // Wait for the user template to render and the label to be updated
    cy.get('#theme-toggle-btn', { timeout: 5000 }).should('exist');
    // The $(function(){}) handler updates the label after template render
    cy.wait(500);
    cy.get('#theme-toggle-btn').should('have.value', 'Light Mode');

    cy.get('#theme-toggle-btn').click();

    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'light');
    cy.get('#theme-toggle-btn').should('have.value', 'Dark Mode');
  });

  it('persists theme preference across page loads', () => {
    cy.setCookie('stormTheme', 'light');
    cy.visit('/');
    cy.get('#theme-toggle-btn', { timeout: 5000 }).click(); // → dark

    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'dark');

    cy.reload();

    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'dark');
  });

  it('dark theme works on topology page', () => {
    cy.setCookie('stormTheme', 'dark');
    cy.visit('/topology.html?id=word-count-1-1234567890');
    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'dark');
  });

  it('dark theme works on visualize page', () => {
    cy.setCookie('stormTheme', 'dark');
    cy.visit('/visualize.html?id=word-count-1-1234567890&sys=false');
    cy.document().its('documentElement')
      .should('have.attr', 'data-bs-theme', 'dark');
  });
});
