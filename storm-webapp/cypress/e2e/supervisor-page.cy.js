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

describe('Storm UI - Supervisor Detail Page', () => {
  beforeEach(() => {
    cy.visit('/supervisor.html?id=supervisor-1');
  });

  it('loads the main bundle', () => {
    cy.get('script[src*="main.bundle.js"]').should('exist');
  });

  it('renders supervisor host info', () => {
    cy.contains('supervisor-host-1').should('exist');
  });

  it('renders the worker table', () => {
    cy.contains('word-count').should('exist');
  });
});
