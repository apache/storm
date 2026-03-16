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

/**
 * Lightweight Express server for Cypress E2E tests.
 *
 * Serves the webpack-built bundles and static HTML/images/templates
 * from the build output directory, and provides mock JSON responses
 * for all Storm REST API endpoints that the UI pages call.
 */

const express = require('express');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 3088;

// --- Static file serving ---
// Webpack bundles are in target/generated-resources/WEB-INF/dist/
// HTML, images, templates are in src/main/java/.../WEB-INF/
const webinfSrc = path.join(
  __dirname,
  '../src/main/java/org/apache/storm/daemon/ui/WEB-INF'
);
const distDir = path.join(
  __dirname,
  '../target/generated-resources/WEB-INF/dist'
);

// Serve webpack bundles at /dist/
app.use('/dist', express.static(distDir));

// Serve images and templates from WEB-INF source
app.use('/images', express.static(path.join(webinfSrc, 'images')));
app.use('/templates', express.static(path.join(webinfSrc, 'templates')));

// --- Load fixture data ---
const fixturesDir = path.join(__dirname, 'fixtures');
function loadFixture(name) {
  return JSON.parse(fs.readFileSync(path.join(fixturesDir, name), 'utf8'));
}

// --- Mock API endpoints ---
app.get('/api/v1/cluster/configuration', (_req, res) => {
  res.json(loadFixture('cluster-configuration.json'));
});

app.get('/api/v1/cluster/summary', (_req, res) => {
  res.json(loadFixture('cluster-summary.json'));
});

app.get('/api/v1/nimbus/summary', (_req, res) => {
  res.json(loadFixture('nimbus-summary.json'));
});

app.get('/api/v1/topology/summary', (_req, res) => {
  res.json(loadFixture('topology-summary.json'));
});

app.get('/api/v1/supervisor/summary', (_req, res) => {
  res.json(loadFixture('supervisor-summary.json'));
});

app.get('/api/v1/owner-resources', (_req, res) => {
  res.json(loadFixture('owner-resources.json'));
});

app.get('/api/v1/topology/:id', (req, res) => {
  res.json(loadFixture('topology-detail.json'));
});

app.get('/api/v1/topology/:id/component/:component', (req, res) => {
  res.json(loadFixture('component-detail.json'));
});

app.get('/api/v1/topology/:id/lag', (_req, res) => {
  res.json({});
});

app.get('/api/v1/topology/:id/logconfig', (_req, res) => {
  res.json({ namedLoggerLevels: {} });
});

app.get('/api/v1/topology/:id/visualization', (_req, res) => {
  res.json(loadFixture('topology-visualization.json'));
});

app.get('/api/v1/supervisor', (_req, res) => {
  res.json(loadFixture('supervisor-detail.json'));
});

// POST endpoints (topology actions) - return success
app.post('/api/v1/topology/:id/:action', (_req, res) => {
  res.json({ status: 'success' });
});

// --- Serve HTML pages ---
// The HTML files use ${packageTimestamp} which we replace on the fly
function serveHtml(filePath, res) {
  let html = fs.readFileSync(filePath, 'utf8');
  html = html.replace(/\$\{packageTimestamp\}/g, 'test');
  res.type('html').send(html);
}

app.get('/', (req, res) => {
  serveHtml(path.join(webinfSrc, 'index.html'), res);
});

// Map each HTML page
const htmlPages = [
  'index', 'topology', 'component', 'supervisor', 'owner',
  'search_result', 'deep_search_result', 'logviewer_search',
  'logviewer', 'visualize', 'flux'
];
htmlPages.forEach((page) => {
  app.get(`/${page}.html`, (req, res) => {
    serveHtml(path.join(webinfSrc, `${page}.html`), res);
  });
});

app.get('/favicon.ico', (req, res) => {
  const faviconPath = path.join(webinfSrc, 'favicon.ico');
  if (fs.existsSync(faviconPath)) {
    res.sendFile(faviconPath);
  } else {
    res.status(204).end();
  }
});

app.listen(PORT, () => {
  console.log(`Storm UI test server running at http://localhost:${PORT}`);
});
