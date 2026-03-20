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
 * Webpack entry point for the topology visualization page (visualize.html).
 */

// --- CSS ---
require('bootstrap/dist/css/bootstrap.min.css');
require('vis/dist/vis.min.css');
require('../css/style.css');

// --- jQuery ---
var $ = require('jquery');
window.$ = $;
window.jQuery = $;

// --- jQuery 4 compatibility shims ---
if (!$.parseJSON) { $.parseJSON = JSON.parse; }
if (!$.trim) { $.trim = function (s) { return s == null ? '' : String(s).trim(); }; }
if (!$.isFunction) { $.isFunction = function (o) { return typeof o === 'function'; }; }

// --- Mustache + jQuery integration ---
var Mustache = require('mustache');
window.Mustache = Mustache;
$.mustache = function (template, view, partials) {
  return Mustache.render(template, view, partials);
};

// --- URL parser ---
var Url = require('domurl');
window.Url = Url;
$.url = function (param) {
  var u = new Url(window.location.href);
  if (param && param.charAt(0) === '?') {
    return u.query[param.substring(1)] || '';
  }
  return u;
};

// --- vis.js ---
var vis = require('vis');
window.vis = vis;

// --- Custom Storm visualization code ---
require('./visualization');
