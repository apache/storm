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
 * Webpack entry point for the Flux topology viewer page (flux.html).
 */

// --- CSS ---
require('bootstrap/dist/css/bootstrap.min.css');
require('../css/style.css');

// --- jQuery ---
var $ = require('jquery');
window.$ = $;
window.jQuery = $;

// --- Cytoscape + dagre layout ---
var cytoscape = require('cytoscape');
var cytoscapeDagre = require('cytoscape-dagre');
cytoscape.use(cytoscapeDagre);
window.cytoscape = cytoscape;

// --- Dagre (exposed as global for cytoscape-dagre) ---
var dagre = require('dagre');
window.dagre = dagre;

// --- Parsing libraries ---
var esprima = require('esprima');
window.esprima = esprima;

var jsyaml = require('js-yaml');
window.jsyaml = jsyaml;
